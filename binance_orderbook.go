package marketdata

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybinanceapi"
	"github.com/robfig/cron/v3"
)

type BinanceOrderBook struct {
	parent          *BinanceMarketData
	SpotOrderBook   *binanceOrderBookBase
	FutureOrderBook *binanceOrderBookBase
	SwapOrderBook   *binanceOrderBookBase
	restQuotaCron   *cron.Cron
}

type binanceOrderBookBase struct {
	parent                    *BinanceOrderBook
	limitRestCountPerMinute   int64
	currentRestCount          int64
	uSpeed                    string
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	initOrderBookSize         int
	BinanceWsClientBase
	Exchange                  Exchange
	AccountType               BinanceAccountType
	OrderBookCacheMap         *MySyncMap[string, *MySyncMap[int64, *mybinanceapi.WsDepth]]
	OrderBookRBTreeMap        *MySyncMap[string, OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientMap               *MySyncMap[string, *mybinanceapi.WsStreamClient]                     //symbol->wsClient
	SubMap                    *MySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]] //symbol->subscribe
	IsInitActionMu            *MySyncMap[string, *sync.Mutex]                                      //symbol->mutex
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]                    //symbol->callback
	depthSubZeroCopy          *MySyncMap[string, bool]                                             //symbol->zeroCopy
	depthResubInFlight        *MySyncMap[string, bool]                                             //symbol->resub in flight
	depthResubAttempt         *MySyncMap[string, int]                                              //symbol->resub attempt, 仅 Ready 后清零
	depthInitEpoch            *MySyncMap[string, int64]                                            //symbol->init generation，bump 后旧 init 退出
	depthLastRestartMilli     *MySyncMap[string, int64]                                            //symbol->最近一次决定重启的时间
	depthResubMu              sync.Mutex
	depthRestartMu            sync.Mutex
	closed                    atomic.Bool
}

// 根据类型获取基础
func (b *BinanceOrderBook) getBaseMapFromAccountType(accountType BinanceAccountType) (*binanceOrderBookBase, error) {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotOrderBook, nil
	case BINANCE_FUTURE:
		return b.FutureOrderBook, nil
	case BINANCE_SWAP:
		return b.SwapOrderBook, nil
	}
	return nil, ErrorAccountType
}

// 新建币安深度基础
func (b *BinanceOrderBook) newBinanceOrderBookBase(config BinanceOrderBookConfigBase) *binanceOrderBookBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 10
	}
	if config.PerConnSubNum == 0 {
		config.PerSubMaxLen = 50
	}
	return &binanceOrderBookBase{
		Exchange: BINANCE,
		BinanceWsClientBase: BinanceWsClientBase{
			perConnSubNum:      config.PerConnSubNum,
			perSubMaxLen:       config.PerSubMaxLen,
			futureWsStreamTier: BinanceFutureWsStreamTierPublic,
			WsClientListMap:    GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
		},
		uSpeed:                    config.USpeed,
		limitRestCountPerMinute:   config.LimitRestCountPerMinute,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		initOrderBookSize:         config.InitOrderBookSize,
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, *MySyncMap[int64, *mybinanceapi.WsDepth]]()),
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		IsInitActionMu:            GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
		depthSubZeroCopy:          GetPointer(NewMySyncMap[string, bool]()),
		depthResubInFlight:        GetPointer(NewMySyncMap[string, bool]()),
		depthResubAttempt:         GetPointer(NewMySyncMap[string, int]()),
		depthInitEpoch:            GetPointer(NewMySyncMap[string, int64]()),
		depthLastRestartMilli:     GetPointer(NewMySyncMap[string, int64]()),
	}

}

// binanceDepthInitLimit REST 快照档位。未配置或非法时默认 100（U 本位 /fapi/v1/depth limit=100 权重 5）。
func binanceDepthInitLimit(initOrderBookSize int) int {
	if initOrderBookSize > 0 {
		return initOrderBookSize
	}
	return 100
}

// 初始化
func (b *BinanceOrderBook) init() {
	c := cron.New(cron.WithSeconds())
	//每隔1分钟刷新一次请求次数
	_, err := c.AddFunc("0 */1 * * * *", func() {
		atomic.StoreInt64(&b.SpotOrderBook.currentRestCount, 0)
		atomic.StoreInt64(&b.FutureOrderBook.currentRestCount, 0)
		atomic.StoreInt64(&b.SwapOrderBook.currentRestCount, 0)
	})
	if err != nil {
		log.Error(err)
		return
	}
	b.restQuotaCron = c
	c.Start()
}

// 获取当前或新建ws客户端
func (b *BinanceOrderBook) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotOrderBook.GetCurrentOrNewWsClient(accountType)
	case BINANCE_FUTURE:
		return b.FutureOrderBook.GetCurrentOrNewWsClient(accountType)
	case BINANCE_SWAP:
		return b.SwapOrderBook.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

// 封装好的获取深度方法
func (b *BinanceOrderBook) GetDepth(BinanceAccountType BinanceAccountType, symbol string, level int, timeoutMilli int64) (*Depth, error) {
	bmap, err := b.getBaseMapFromAccountType(BinanceAccountType)
	if err != nil {
		return nil, err
	}

	depth, ok := bmap.OrderBookMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s depth not found", symbol)
		return nil, err
	}
	orderBook, ok := bmap.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
		log.Error(err)
		return nil, err
	}

	newDepth, err := orderBook.LoadToDepth(depth, level)
	if err != nil {
		return nil, err
	}
	//如果超时限制大于0 判断深度是否超时
	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
		err := fmt.Errorf("symbol:%s depth timeout", symbol)
		return newDepth, err
	}
	return newDepth, nil
}

// 封装好的获取深度方法(高性能视图模式)
func (b *BinanceOrderBook) ViewDepth(BinanceAccountType BinanceAccountType, symbol string, level int, timeoutMilli int64, bizLogic func(*Depth) error) error {
	bmap, err := b.getBaseMapFromAccountType(BinanceAccountType)
	if err != nil {
		return err
	}

	depth, ok := bmap.OrderBookMap.Load(symbol)
	if !ok {
		return fmt.Errorf("symbol:%s depth not found", symbol)
	}
	orderBook, ok := bmap.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
		log.Error(err)
		return err
	}

	return orderBook.ViewDepth(depth, level, func(d *Depth) error {
		//如果超时限制大于0 判断深度是否超时
		if timeoutMilli > 0 && time.Now().UnixMilli()-d.Timestamp > timeoutMilli {
			return fmt.Errorf("symbol:%s depth timeout", symbol)
		}
		return bizLogic(d)
	})
}

// 订阅币安深度底层执行
func (b *binanceOrderBookBase) subscribeBinanceDepthMultipleWithZeroCopy(binanceWsClient *mybinanceapi.WsStreamClient, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {

	binanceSub, err := binanceWsClient.SubscribeIncrementDepthMultiple(symbols, b.uSpeed)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		b.WsClientMap.Store(symbol, binanceWsClient)
		b.SubMap.Store(symbol, binanceSub)
		b.CallBackMap.Store(symbol, callback)
		b.depthSubZeroCopy.Store(symbol, isZeroCopy)
		if _, ok := b.IsInitActionMu.Load(symbol); !ok {
			b.IsInitActionMu.Store(symbol, &sync.Mutex{})
		}
	}
	go func() {
		for {
			// log.Info("next binanceSub...")
			select {
			case err := <-binanceSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-binanceSub.ResultChan():
				Symbol := result.Symbol
				//检测深度是否初始化
				_, err := b.checkBinanceDepthIsReady(Symbol)
				if err != nil {
					//直接存入深度缓存
					b.saveBinanceDepthCache(result)
					continue
				}

				//判断是否丢包；官方要求从 REST 快照步骤重新初始化，并重新开始缓冲。
				if lastLowerU, ok := b.OrderBookLastUpdateIdMap.Load(Symbol); ok {
					if b.AccountType == BINANCE_SPOT {
						if result.UpperU > lastLowerU+1 {
							b.resetBinanceDepthLocalState(Symbol, true)
							b.saveBinanceDepthCache(result)
							go func() {
								err := b.initBinanceDepthFunc(result.Symbol)
								if err != nil {
									log.Error(err)
								}
							}()
							continue
						} else if binanceSpotDepthBridges(result.UpperU, result.LowerU, lastLowerU) {
							// 首个可衔接包
						}
					} else {
						if result.LowerU <= lastLowerU {
							// 过期/重复增量，直接丢弃（避免同一包重放把 pu!=last_u 误判为丢包）
							continue
						}
						if result.PreU != lastLowerU {
							// 官方: 新 event 的 pu 应等于上一 event 的 u，否则从快照步骤重同步
							b.resetBinanceDepthLocalState(Symbol, true)
							b.saveBinanceDepthCache(result)
							go func() {
								err := b.initBinanceDepthFunc(result.Symbol)
								if err != nil {
									log.Error(err)
								}
							}()
							continue
						}
					}
				}

				//log.Warn(result.LowerU, result.UpperU, result.LastUpdateID)

				//保存至OrderBook
				err = b.saveBinanceDepthOrderBook(result)
				if err != nil {
					log.Error(err)
					continue
				}

				if callback == nil || b.callBackDepthLevel == 0 {
					continue
				}
				if isZeroCopy {
					//高性能查询盘口并执行回调
					err = b.parent.ViewDepth(b.AccountType, Symbol, int(b.callBackDepthLevel), b.callBackDepthTimeoutMilli, func(d *Depth) error {
						d.UId, d.PreUId = b.GetUidAndPreUid(result)
						callback(d, nil)
						return nil
					})
					if err != nil {
						callback(nil, err)
						continue
					}
				} else {
					depth, err := b.parent.GetDepth(b.AccountType, Symbol, int(b.callBackDepthLevel), b.callBackDepthTimeoutMilli)
					if err != nil {
						callback(nil, err)
						continue
					}
					depth.UId, depth.PreUId = b.GetUidAndPreUid(result)
					callback(depth, nil)
				}
			case <-binanceSub.CloseChan():
				if b.closed.Load() {
					return
				}
				if !b.binanceDepthSubStillActive(binanceSub, symbols) {
					log.Infof("币安深度订阅已关闭且已被替换，跳过重订 params=%v", binanceSub.Params)
					return
				}
				log.Warnf("币安深度订阅已关闭，准备重订 params=%v", binanceSub.Params)
				symbolsCopy := append([]string(nil), symbols...)
				go b.resubscribeBinanceDepthAfterClose(binanceWsClient, symbolsCopy, callback, isZeroCopy)
				return
			}
		}
	}()

	log.Info("订阅成功, 开始初始化深度池...")
	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(binanceWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(binanceWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	// 订阅与桥接解耦：不等待 init 完成，避免单个 symbol 卡在 cache_behind 时堵住后续订阅。
	for _, symbol := range symbols {
		symbol := symbol
		go func() {
			if err := b.initBinanceDepthFunc(symbol); err != nil {
				log.Error(err)
			}
		}()
	}
	return nil
}

//// 取消订阅币安深度
//func (b *binanceOrderBookBase) UnSubscribeBinanceDepth(Symbol string) error {
//	binanceSub, ok := b.SubMap.Load(Symbol)
//	if !ok {
//		return nil
//	}
//	return binanceSub.Unsubscribe()
//}
//
//// 重新订阅币安深度
//func (b *binanceOrderBookBase) ReSubscribeBinanceDepth(Symbol string) error {
//	err := b.UnSubscribeBinanceDepth(Symbol)
//	if err != nil {
//		return err
//	}
//	binanceWsClient, ok := b.WsClientMap.Load(Symbol)
//	if !ok {
//		err := fmt.Errorf("symbol:%s binanceWsClient not found", Symbol)
//		return err
//	}
//	callBack, ok := b.CallBackMap.Load(Symbol)
//	if !ok {
//		err := fmt.Errorf("symbol:%s callBack not found", Symbol)
//		return err
//	}
//	return b.subscribeBinanceDepthMultiple(binanceWsClient, []string{Symbol}, callBack)
//}

// resetBinanceDepthLocalState 清空本地盘口状态；clearCache=true 时同时丢弃增量缓存（官方重同步从重新缓冲开始）。
func (b *binanceOrderBookBase) resetBinanceDepthLocalState(symbol string, clearCache bool) {
	b.OrderBookReadyUpdateIdMap.Delete(symbol)
	b.OrderBookMap.Delete(symbol)
	b.OrderBookLastUpdateIdMap.Delete(symbol)
	b.OrderBookRBTreeMap.Delete(symbol)
	if clearCache {
		b.OrderBookCacheMap.Delete(symbol)
	}
}

func (b *binanceOrderBookBase) invalidateBinanceDepthReadyKeepSnapshot(symbol string) {
	b.OrderBookReadyUpdateIdMap.Delete(symbol)
	b.OrderBookCacheMap.Delete(symbol)
}

// 初始化币安深度。官方顺序：先缓冲增量，再拉 REST 快照，再按 U/u（及 FUTURE pu）桥接。
func (b *binanceOrderBookBase) initBinanceDepthFunc(symbol string) error {
	if b.closed.Load() {
		return nil
	}
	mu, ok := b.IsInitActionMu.Load(symbol)
	if !ok {
		mu = &sync.Mutex{}
		b.IsInitActionMu.Store(symbol, mu)
	}
	if !mu.TryLock() {
		return nil
	}
	defer mu.Unlock()

	aheadAttempt := 0
	restFailAttempt := 0
	epoch := b.currentDepthInitEpoch(symbol)

snapshotLoop:
	for {
		if b.depthInitAborted(symbol, epoch) {
			return nil
		}
		if b.depthCacheLen(symbol) >= binanceDepthCacheMaxEvents {
			log.Warnf("%s depth cache overflow len=%d, restart buffer", symbol, b.depthCacheLen(symbol))
			b.resetBinanceDepthLocalState(symbol, true)
			time.Sleep(binanceDepthRestartBackoff)
			continue
		}
		// 官方第 2 步：尽量先等到至少一包增量，再拉快照（低活跃盘口超时后仍拉 REST）。
		if b.depthCacheLen(symbol) == 0 {
			if _, hasSnapshot := b.OrderBookLastUpdateIdMap.Load(symbol); !hasSnapshot {
				b.waitForFirstDepthCacheEvent(symbol, epoch, binanceDepthWaitFirstEvent)
			}
		}

		if _, hasSnapshot := b.OrderBookLastUpdateIdMap.Load(symbol); !hasSnapshot {
			err := b.initBinanceDepthOrderBook(symbol, epoch)
			if err != nil {
				if errors.Is(err, errBinanceDepthInitAborted) {
					return nil
				}
				restFailAttempt++
				backoff := binanceDepthBackoffDuration(restFailAttempt-1, time.Second, 5*time.Second)
				log.Infof("重新初始化币安深度: %s err=%v backoff=%s", symbol, err, backoff)
				time.Sleep(backoff)
				continue
			}
		}
		restFailAttempt = 0

		waitCount := 0
		stallCount := 0
		prevCacheLen := -1
		prevMaxLowerU := int64(0)
		for {
			if b.depthInitAborted(symbol, epoch) {
				return nil
			}
			err := b.saveBinanceDepthOrderBookFromCache(symbol)
			if err == nil {
				if b.depthResubAttempt != nil {
					b.depthResubAttempt.Delete(symbol)
				}
				return nil
			}

			var bridgeErr *binanceDepthBridgeError
			if !errors.As(err, &bridgeErr) {
				log.Error(err)
				b.resetBinanceDepthLocalState(symbol, true)
				time.Sleep(binanceDepthRestartBackoff)
				log.Info("重新初始化币安深度(桥接失败): ", symbol)
				continue snapshotLoop
			}

			if binanceDepthCacheNeedsRestart(bridgeErr.CacheLen, binanceDepthCacheMaxEvents) {
				log.Warnf("%s depth cache overflow during bridge cacheLen=%d, restart buffer", symbol, bridgeErr.CacheLen)
				b.resetBinanceDepthLocalState(symbol, true)
				time.Sleep(binanceDepthRestartBackoff)
				continue snapshotLoop
			}

			switch decideBinanceDepthBridgeMissAction(bridgeErr.Reason) {
			case binanceDepthMissWaitSameSnapshot:
				waitCount++
				stallCount = binanceDepthWaitStallCount(prevCacheLen, bridgeErr.CacheLen, prevMaxLowerU, bridgeErr.MaxLowerU, stallCount)
				prevCacheLen = bridgeErr.CacheLen
				prevMaxLowerU = bridgeErr.MaxLowerU
				if waitCount == 1 || waitCount%40 == 0 {
					log.Infof("%s depth bridge waiting reason=%s lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d waitCount=%d stallCount=%d",
						bridgeErr.Symbol, bridgeErr.Reason, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
						bridgeErr.MinUpperU, bridgeErr.MaxLowerU, waitCount, stallCount)
				}
				if decideBinanceDepthSameSnapshotWait(waitCount, stallCount, binanceDepthSameSnapshotMaxWait, binanceDepthSameSnapshotStallLimit) == binanceDepthResubKeepSnapshot {
					log.Warnf("%s depth bridge cache frozen, resub WS keep snapshot lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d waitCount=%d stallCount=%d",
						bridgeErr.Symbol, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
						bridgeErr.MinUpperU, bridgeErr.MaxLowerU, waitCount, stallCount)
					b.invalidateBinanceDepthReadyKeepSnapshot(symbol)
					b.restartBinanceDepthStream(symbol)
					// 必须退出以释放 IsInitActionMu，否则重订后的新 init 会 TryLock 失败而无人桥接。
					return nil
				}
				time.Sleep(binanceDepthSameSnapshotPoll)
			case binanceDepthMissRefetchKeepCache:
				aheadAttempt++
				backoff := binanceDepthBackoffDuration(aheadAttempt-1, binanceDepthAheadBackoffMin, binanceDepthAheadBackoffMax)
				log.Warnf("%s depth bridge cache_ahead lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d attempt=%d backoff=%s",
					bridgeErr.Symbol, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
					bridgeErr.MinUpperU, bridgeErr.MaxLowerU, aheadAttempt, backoff)
				// 保留增量缓存，丢掉旧快照 id，强制走 REST 拉新快照。
				b.OrderBookLastUpdateIdMap.Delete(symbol)
				time.Sleep(backoff)
				continue snapshotLoop
			default:
				log.Warnf("%s depth bridge restart reason=%s lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d",
					bridgeErr.Symbol, bridgeErr.Reason, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
					bridgeErr.MinUpperU, bridgeErr.MaxLowerU)
				b.resetBinanceDepthLocalState(symbol, true)
				time.Sleep(binanceDepthRestartBackoff)
				aheadAttempt = 0
				continue snapshotLoop
			}
		}
	}
}

// 检测深度是否准备好
func (b *binanceOrderBookBase) checkBinanceDepthIsReady(Symbol string) (int64, error) {
	readyId, isReady := b.OrderBookReadyUpdateIdMap.Load(Symbol)
	if !isReady {
		err := fmt.Errorf("%s 深度未准备好", Symbol)
		return 0, err
	}
	return readyId, nil
}

// 初始化深度
func (b *binanceOrderBookBase) initBinanceDepthOrderBook(Symbol string, epoch int64) error {
	if !b.acquireBinanceDepthRestQuota(Symbol, epoch) {
		return errBinanceDepthInitAborted
	}
	orderBook, ok := b.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		b.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	switch b.AccountType {
	case BINANCE_SPOT:
		//重新初始化
		depth, err := binance.NewSpotRestClient("", "").NewSpotDepth().Symbol(Symbol).Limit(binanceDepthInitLimit(b.initOrderBookSize)).Do()
		if err != nil {
			log.Error(err)
			return err
		}

		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))

		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, q, err := bid.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			bidPrices = append(bidPrices, p.InexactFloat64())
			bidQuantities = append(bidQuantities, q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, q, err := ask.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			askPrices = append(askPrices, p.InexactFloat64())
			askQuantities = append(askQuantities, q.InexactFloat64())
		}
		orderBook.PutBidLevels(bidPrices, bidQuantities)
		orderBook.PutAskLevels(askPrices, askQuantities)
		b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
	case BINANCE_FUTURE:
		//重新初始化（档位由 InitOrderBookSize 决定；100 档权重 5，1000 档权重 20）
		depth, err := binance.NewFutureRestClient("", "").NewFutureDepth().Symbol(Symbol).Limit(binanceDepthInitLimit(b.initOrderBookSize)).Do()
		if err != nil {
			log.Error(err)
			return err
		}

		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))

		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, q, err := bid.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			bidPrices = append(bidPrices, p.InexactFloat64())
			bidQuantities = append(bidQuantities, q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, q, err := ask.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			askPrices = append(askPrices, p.InexactFloat64())
			askQuantities = append(askQuantities, q.InexactFloat64())
		}

		orderBook.PutBidLevels(bidPrices, bidQuantities)
		orderBook.PutAskLevels(askPrices, askQuantities)
		b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
	case BINANCE_SWAP:
		//重新初始化（档位由 InitOrderBookSize 决定，与 U 本位一致）
		depth, err := binance.NewSwapRestClient("", "").NewSwapDepth().Symbol(Symbol).Limit(binanceDepthInitLimit(b.initOrderBookSize)).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, q, err := bid.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			bidPrices = append(bidPrices, p.InexactFloat64())
			bidQuantities = append(bidQuantities, q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, q, err := ask.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			askPrices = append(askPrices, p.InexactFloat64())
			askQuantities = append(askQuantities, q.InexactFloat64())
		}

		orderBook.PutBidLevels(bidPrices, bidQuantities)
		orderBook.PutAskLevels(askPrices, askQuantities)

		b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
	}

	// log.Info(b.OrderBookLastUpdateIdMap.Load(Symbol))
	return nil
}

// 将Depth缓存保存至OrderBook
func (b *binanceOrderBookBase) saveBinanceDepthOrderBookFromCache(Symbol string) error {

	// readyId, err := b.checkBinanceDepthIsReady(Symbol)
	// if err != nil {
	// 	log.Error(err)
	// 	return err
	// }
	lastUpdateId, ok := b.OrderBookLastUpdateIdMap.Load(Symbol)
	if !ok {
		err := fmt.Errorf("%s lastUpdateId not found", Symbol)
		log.Error(err)
		return err
	}
	// log.Info(lastUpdateId)

	//读取缓存到OrderBook
	cacheMap, ok := b.OrderBookCacheMap.Load(Symbol)
	if !ok {
		newMap := NewMySyncMap[int64, *mybinanceapi.WsDepth]()
		cacheMap = &newMap
		b.OrderBookCacheMap.Store(Symbol, cacheMap)
	}

	//按照LowerU排序
	var cacheList []mybinanceapi.WsDepth
	cacheMap.Range(func(k int64, v *mybinanceapi.WsDepth) bool {
		cacheList = append(cacheList, *v)
		return true
	})
	sort.Sort(SortBinanceWsDepthSlice(cacheList))

	bridgeIdx := findBinanceDepthBridgeIndex(b.AccountType, cacheList, lastUpdateId)
	if bridgeIdx < 0 {
		minUpperU, maxLowerU := binanceDepthCacheBounds(cacheList)
		reason := classifyBinanceDepthBridgeMiss(b.AccountType, cacheList, lastUpdateId)
		return &binanceDepthBridgeError{
			Symbol:       Symbol,
			LastUpdateId: lastUpdateId,
			CacheLen:     len(cacheList),
			MinUpperU:    minUpperU,
			MaxLowerU:    maxLowerU,
			Reason:       reason,
		}
	}

	appliedLast, hole, err := applyBinanceDepthCacheContiguous(b.AccountType, lastUpdateId, cacheList, bridgeIdx, func(v mybinanceapi.WsDepth) error {
		return b.saveBinanceDepthOrderBook(v)
	})
	if err != nil {
		log.Error(err)
		return err
	}
	if hole {
		minUpperU, maxLowerU := binanceDepthCacheBounds(cacheList)
		return &binanceDepthBridgeError{
			Symbol:       Symbol,
			LastUpdateId: lastUpdateId,
			CacheLen:     len(cacheList),
			MinUpperU:    minUpperU,
			MaxLowerU:    maxLowerU,
			Reason:       binanceDepthBridgeMissNoCovering,
		}
	}
	lastUpdateId = appliedLast

	// 先标 Ready，再排空并发写入的缓存，避免漏包后 pu 断裂。
	b.OrderBookReadyUpdateIdMap.Store(Symbol, lastUpdateId)
	if drainErr := b.drainBinanceDepthCacheAfterReady(Symbol); drainErr != nil {
		return drainErr
	}
	b.OrderBookCacheMap.Delete(Symbol)

	return nil
}

// 将Depth缓存
func (b *binanceOrderBookBase) saveBinanceDepthCache(result mybinanceapi.WsDepth) {
	Symbol := result.Symbol

	cacheMap, ok := b.OrderBookCacheMap.Load(Symbol)
	if !ok {
		newMap := NewMySyncMap[int64, *mybinanceapi.WsDepth]()
		cacheMap = &newMap
		b.OrderBookCacheMap.Store(Symbol, cacheMap)
	}
	if cacheMap.Length() >= binanceDepthCacheMaxEvents {
		cacheMap.Clear()
	}
	dup := new(mybinanceapi.WsDepth)
	*dup = result
	cacheMap.Store(dup.LowerU, dup)
}

// 将Depth保存至OrderBook
func (b *binanceOrderBookBase) saveBinanceDepthOrderBook(result mybinanceapi.WsDepth) error {
	Symbol := result.Symbol

	b.OrderBookLastUpdateIdMap.Store(Symbol, result.LowerU)

	orderBook, ok := b.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		b.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}

	bidPrices := make([]float64, 0, len(result.Bids))
	bidQuantities := make([]float64, 0, len(result.Bids))
	askPrices := make([]float64, 0, len(result.Asks))
	askQuantities := make([]float64, 0, len(result.Asks))

	for _, bid := range result.Bids {
		bidPrices = append(bidPrices, bid.Price)
		bidQuantities = append(bidQuantities, bid.Quantity)
	}

	for _, ask := range result.Asks {
		askPrices = append(askPrices, ask.Price)
		askQuantities = append(askQuantities, ask.Quantity)
	}

	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)

	//log.Warn(result.LowerU, result.UpperU, result.LastUpdateID)

	UId, PreUId := b.GetUidAndPreUid(result)
	now := time.Now().UnixMilli()
	targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType)
	if targetTs > now {
		targetTs = now
	}
	depth := &Depth{
		UId:         UId,
		PreUId:      PreUId,
		AccountType: string(b.AccountType),
		Exchange:    string(b.Exchange),
		Symbol:      result.Symbol,
		Timestamp:   targetTs,
	}
	b.OrderBookMap.Store(Symbol, depth)

	return nil
}

// binanceSpotDepthBridges 现货官方桥接条件: U <= lastUpdateId+1 <= u
func binanceSpotDepthBridges(upperU, lowerU, lastUpdateId int64) bool {
	return upperU <= lastUpdateId+1 && lowerU >= lastUpdateId+1
}

// binanceFutureDepthBridges 合约官方桥接条件: U <= lastUpdateId <= u
func binanceFutureDepthBridges(upperU, lowerU, lastUpdateId int64) bool {
	return upperU <= lastUpdateId && lowerU >= lastUpdateId
}

type binanceDepthBridgeMissReason string

const (
	binanceDepthBridgeMissNone        binanceDepthBridgeMissReason = "none"
	binanceDepthBridgeMissEmptyCache  binanceDepthBridgeMissReason = "empty_cache"
	binanceDepthBridgeMissCacheBehind binanceDepthBridgeMissReason = "cache_behind" // max(u) < target，可短等追赶
	binanceDepthBridgeMissCacheAhead  binanceDepthBridgeMissReason = "cache_ahead"  // min(U) > target，快照过旧或丢包空洞
	binanceDepthBridgeMissNoCovering  binanceDepthBridgeMissReason = "no_covering"  // 缓存跨越 target 但无覆盖事件（中间空洞）
)

type binanceDepthBridgeError struct {
	Symbol       string
	LastUpdateId int64
	CacheLen     int
	MinUpperU    int64
	MaxLowerU    int64
	Reason       binanceDepthBridgeMissReason
}

func (e *binanceDepthBridgeError) Error() string {
	return fmt.Sprintf("%s depth bridge event not found for lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d reason=%s",
		e.Symbol, e.LastUpdateId, e.CacheLen, e.MinUpperU, e.MaxLowerU, e.Reason)
}

func binanceDepthBridgeTargetID(accountType BinanceAccountType, lastUpdateId int64) int64 {
	if accountType == BINANCE_SPOT {
		return lastUpdateId + 1
	}
	return lastUpdateId
}

func binanceDepthCacheBounds(cacheList []mybinanceapi.WsDepth) (minUpperU, maxLowerU int64) {
	if len(cacheList) == 0 {
		return 0, 0
	}
	minUpperU = cacheList[0].UpperU
	maxLowerU = cacheList[0].LowerU
	for i := 1; i < len(cacheList); i++ {
		if cacheList[i].UpperU < minUpperU {
			minUpperU = cacheList[i].UpperU
		}
		if cacheList[i].LowerU > maxLowerU {
			maxLowerU = cacheList[i].LowerU
		}
	}
	return minUpperU, maxLowerU
}

// classifyBinanceDepthBridgeMiss 在找不到 bridge 时区分缓存落后/超前/中间空洞。
// empty/behind：同快照继续等；ahead：保留缓冲重取快照；no_covering：清缓存重同步。
func classifyBinanceDepthBridgeMiss(accountType BinanceAccountType, cacheList []mybinanceapi.WsDepth, lastUpdateId int64) binanceDepthBridgeMissReason {
	if findBinanceDepthBridgeIndex(accountType, cacheList, lastUpdateId) >= 0 {
		return binanceDepthBridgeMissNone
	}
	if len(cacheList) == 0 {
		return binanceDepthBridgeMissEmptyCache
	}
	target := binanceDepthBridgeTargetID(accountType, lastUpdateId)
	minUpperU, maxLowerU := binanceDepthCacheBounds(cacheList)
	if maxLowerU < target {
		return binanceDepthBridgeMissCacheBehind
	}
	if minUpperU > target {
		return binanceDepthBridgeMissCacheAhead
	}
	return binanceDepthBridgeMissNoCovering
}

func shouldClearDepthCacheOnBridgeMiss(reason binanceDepthBridgeMissReason) bool {
	return decideBinanceDepthBridgeMissAction(reason) == binanceDepthMissRestartClearCache
}

func binanceDepthBridgeMissWorthWaiting(reason binanceDepthBridgeMissReason) bool {
	return decideBinanceDepthBridgeMissAction(reason) == binanceDepthMissWaitSameSnapshot
}

const (
	binanceDepthCacheMaxEvents         = 4096
	binanceDepthWaitFirstEvent         = 2 * time.Second
	binanceDepthSameSnapshotPoll       = 50 * time.Millisecond
	binanceDepthSameSnapshotMaxWait    = 0  // 缓存仍在增长则继续等，禁止因等待时长去 REST
	binanceDepthSameSnapshotStallLimit = 40 // 2s：cacheLen/maxLowerU 不动则重订 WS，保留快照
	binanceDepthQuotaPoll              = 200 * time.Millisecond
	binanceDepthAheadBackoffMin        = 200 * time.Millisecond
	binanceDepthAheadBackoffMax        = 2 * time.Second
	binanceDepthRestartBackoff         = time.Second
	binanceDepthUnsubscribeTimeout     = 2 * time.Second
	binanceDepthResubBackoffMin        = 2 * time.Second
	binanceDepthResubBackoffMax        = 60 * time.Second
	binanceDepthRestartDebounce        = 3 * time.Second
	binanceDepthInitIdleWait           = 15 * time.Second
)

var errBinanceDepthUnsubscribeTimeout = errors.New("binance depth unsubscribe timeout")
var errBinanceDepthInitAborted = errors.New("binance depth init aborted")

type binanceDepthMissAction int

const (
	binanceDepthMissWaitSameSnapshot binanceDepthMissAction = iota
	binanceDepthMissRefetchKeepCache
	binanceDepthMissRestartClearCache
)

type binanceDepthSameSnapshotWaitDecision int

const (
	binanceDepthKeepWaiting binanceDepthSameSnapshotWaitDecision = iota
	binanceDepthResubKeepSnapshot
	binanceDepthAbandonWaitTimeout
)

func decideBinanceDepthBridgeMissAction(reason binanceDepthBridgeMissReason) binanceDepthMissAction {
	switch reason {
	case binanceDepthBridgeMissEmptyCache, binanceDepthBridgeMissCacheBehind:
		return binanceDepthMissWaitSameSnapshot
	case binanceDepthBridgeMissCacheAhead:
		return binanceDepthMissRefetchKeepCache
	default:
		return binanceDepthMissRestartClearCache
	}
}

func binanceDepthWaitStallCount(prevLen, curLen int, prevMaxU, curMaxU int64, stallCount int) int {
	if prevLen >= 0 && curLen == prevLen && curMaxU == prevMaxU {
		return stallCount + 1
	}
	return 0
}

func decideBinanceDepthSameSnapshotWait(waitCount, stallCount, maxWait, stallLimit int) binanceDepthSameSnapshotWaitDecision {
	if stallLimit > 0 && stallCount >= stallLimit {
		return binanceDepthResubKeepSnapshot
	}
	if maxWait > 0 && waitCount >= maxWait {
		return binanceDepthAbandonWaitTimeout
	}
	return binanceDepthKeepWaiting
}

func binanceDepthCacheNeedsRestart(cacheLen, cap int) bool {
	return cap > 0 && cacheLen >= cap
}

func binanceDepthBackoffDuration(attempt int, min, max time.Duration) time.Duration {
	if min <= 0 {
		min = time.Millisecond
	}
	if max < min {
		max = min
	}
	if attempt < 0 {
		attempt = 0
	}
	d := min
	for i := 0; i < attempt; i++ {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

func binanceDepthResubBackoffAfterAttempt(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	return binanceDepthBackoffDuration(attempt-2, binanceDepthResubBackoffMin, binanceDepthResubBackoffMax)
}

func binanceDepthAnyResubInFlight(inFlight *MySyncMap[string, bool], symbols []string) bool {
	if inFlight == nil {
		return false
	}
	for _, s := range symbols {
		if s == "" {
			continue
		}
		if _, ok := inFlight.Load(s); ok {
			return true
		}
	}
	return false
}

func binanceDepthGroupRecentlyRestarted(last *MySyncMap[string, int64], symbols []string, nowMilli int64, window time.Duration) bool {
	if last == nil || window <= 0 {
		return false
	}
	lim := window.Milliseconds()
	for _, s := range symbols {
		if s == "" {
			continue
		}
		t, ok := last.Load(s)
		if ok && nowMilli-t < lim {
			return true
		}
	}
	return false
}

func stampBinanceDepthGroupRestarted(last *MySyncMap[string, int64], symbols []string, nowMilli int64) {
	if last == nil {
		return
	}
	for _, s := range symbols {
		if s == "" {
			continue
		}
		last.Store(s, nowMilli)
	}
}

func binanceDepthSymbolSet(symbols []string) map[string]struct{} {
	m := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		if s == "" {
			continue
		}
		m[s] = struct{}{}
	}
	return m
}

func binanceDepthWsClientStillUsed(
	clients *MySyncMap[string, *mybinanceapi.WsStreamClient],
	client *mybinanceapi.WsStreamClient,
	exclude map[string]struct{},
) bool {
	if clients == nil || client == nil {
		return false
	}
	used := false
	clients.Range(func(symbol string, c *mybinanceapi.WsStreamClient) bool {
		if _, skip := exclude[symbol]; skip {
			return true
		}
		if c == client {
			used = true
			return false
		}
		return true
	})
	return used
}

func unsubscribeBinanceDepthSubWithTimeout(unsub func() error, timeout time.Duration) error {
	if unsub == nil {
		return nil
	}
	if timeout <= 0 {
		return unsub()
	}
	done := make(chan error, 1)
	go func() {
		done <- unsub()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errBinanceDepthUnsubscribeTimeout
	}
}

type binanceDepthFollowAction int

const (
	binanceDepthFollowApply binanceDepthFollowAction = iota
	binanceDepthFollowSkip
	binanceDepthFollowHole
)

// binanceDepthEventFollows 判断已同步后的下一包：过期跳过、可衔接则 apply、否则空洞。
func binanceDepthEventFollows(accountType BinanceAccountType, lastUpdateId int64, v mybinanceapi.WsDepth) binanceDepthFollowAction {
	if accountType == BINANCE_SPOT {
		if v.LowerU < lastUpdateId+1 {
			return binanceDepthFollowSkip
		}
		if binanceSpotDepthBridges(v.UpperU, v.LowerU, lastUpdateId) {
			return binanceDepthFollowApply
		}
		return binanceDepthFollowHole
	}
	if v.LowerU <= lastUpdateId {
		return binanceDepthFollowSkip
	}
	pre := v.PreU
	if pre == 0 {
		pre = v.UpperU - 1
	}
	if pre == lastUpdateId {
		return binanceDepthFollowApply
	}
	return binanceDepthFollowHole
}

func applyBinanceDepthCacheContiguous(
	accountType BinanceAccountType,
	lastUpdateId int64,
	cacheList []mybinanceapi.WsDepth,
	startIdx int,
	apply func(mybinanceapi.WsDepth) error,
) (int64, bool, error) {
	if startIdx < 0 || startIdx >= len(cacheList) {
		return lastUpdateId, false, nil
	}
	current := lastUpdateId
	for i := startIdx; i < len(cacheList); i++ {
		v := cacheList[i]
		if i > startIdx {
			switch binanceDepthEventFollows(accountType, current, v) {
			case binanceDepthFollowSkip:
				continue
			case binanceDepthFollowHole:
				return current, true, nil
			}
		}
		if err := apply(v); err != nil {
			return current, false, err
		}
		current = v.LowerU
	}
	return current, false, nil
}

func (b *binanceOrderBookBase) acquireBinanceDepthRestQuota(symbol string, epoch int64) bool {
	if b.limitRestCountPerMinute <= 0 {
		return !b.depthInitAborted(symbol, epoch)
	}
	logged := false
	for {
		if b.depthInitAborted(symbol, epoch) {
			return false
		}
		cur := atomic.LoadInt64(&b.currentRestCount)
		if cur >= b.limitRestCountPerMinute {
			if !logged {
				log.Infof("币安深度REST配额已满 %d/%d，等待窗口恢复", cur, b.limitRestCountPerMinute)
				logged = true
			}
			time.Sleep(binanceDepthQuotaPoll)
			continue
		}
		if atomic.CompareAndSwapInt64(&b.currentRestCount, cur, cur+1) {
			return true
		}
	}
}

func (b *binanceOrderBookBase) depthCacheLen(symbol string) int {
	cacheMap, ok := b.OrderBookCacheMap.Load(symbol)
	if !ok || cacheMap == nil {
		return 0
	}
	return cacheMap.Length()
}

func (b *binanceOrderBookBase) waitForFirstDepthCacheEvent(symbol string, epoch int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if b.depthInitAborted(symbol, epoch) {
			return false
		}
		if b.depthCacheLen(symbol) > 0 {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (b *binanceOrderBookBase) loadSortedBinanceDepthCache(symbol string) []mybinanceapi.WsDepth {
	cacheMap, ok := b.OrderBookCacheMap.Load(symbol)
	if !ok || cacheMap == nil {
		return nil
	}
	var cacheList []mybinanceapi.WsDepth
	cacheMap.Range(func(k int64, v *mybinanceapi.WsDepth) bool {
		cacheList = append(cacheList, *v)
		return true
	})
	sort.Sort(SortBinanceWsDepthSlice(cacheList))
	return cacheList
}

func (b *binanceOrderBookBase) drainBinanceDepthCacheAfterReady(symbol string) error {
	lastUpdateId, ok := b.OrderBookLastUpdateIdMap.Load(symbol)
	if !ok {
		return fmt.Errorf("%s lastUpdateId not found", symbol)
	}
	cacheList := b.loadSortedBinanceDepthCache(symbol)
	if len(cacheList) == 0 {
		return nil
	}
	start := -1
	for i, v := range cacheList {
		switch binanceDepthEventFollows(b.AccountType, lastUpdateId, v) {
		case binanceDepthFollowSkip:
			continue
		case binanceDepthFollowHole:
			minUpperU, maxLowerU := binanceDepthCacheBounds(cacheList)
			b.OrderBookReadyUpdateIdMap.Delete(symbol)
			return &binanceDepthBridgeError{
				Symbol:       symbol,
				LastUpdateId: lastUpdateId,
				CacheLen:     len(cacheList),
				MinUpperU:    minUpperU,
				MaxLowerU:    maxLowerU,
				Reason:       binanceDepthBridgeMissNoCovering,
			}
		default:
			start = i
		}
		break
	}
	if start < 0 {
		return nil
	}
	appliedLast, hole, err := applyBinanceDepthCacheContiguous(b.AccountType, lastUpdateId, cacheList, start, func(v mybinanceapi.WsDepth) error {
		return b.saveBinanceDepthOrderBook(v)
	})
	if err != nil {
		return err
	}
	if hole {
		minUpperU, maxLowerU := binanceDepthCacheBounds(cacheList)
		b.OrderBookReadyUpdateIdMap.Delete(symbol)
		return &binanceDepthBridgeError{
			Symbol:       symbol,
			LastUpdateId: lastUpdateId,
			CacheLen:     len(cacheList),
			MinUpperU:    minUpperU,
			MaxLowerU:    maxLowerU,
			Reason:       binanceDepthBridgeMissNoCovering,
		}
	}
	b.OrderBookReadyUpdateIdMap.Store(symbol, appliedLast)
	return nil
}

func findBinanceDepthBridgeIndex(accountType BinanceAccountType, cacheList []mybinanceapi.WsDepth, lastUpdateId int64) int {
	for index, v := range cacheList {
		if accountType == BINANCE_SPOT {
			if binanceSpotDepthBridges(v.UpperU, v.LowerU, lastUpdateId) {
				return index
			}
			continue
		}
		if binanceFutureDepthBridges(v.UpperU, v.LowerU, lastUpdateId) {
			return index
		}
	}
	return -1
}

func (b *binanceOrderBookBase) GetUidAndPreUid(result mybinanceapi.WsDepth) (int64, int64) {
	UId := int64(0)
	if result.LastUpdateID != 0 {
		UId = result.LastUpdateID
	} else if result.LowerU != 0 {
		UId = result.LowerU
	}

	PreUId := int64(0)

	PreUId = result.PreU

	if PreUId == 0 {
		PreUId = result.UpperU - 1
	}
	return UId, PreUId
}

// 订阅深度
func (b *BinanceOrderBook) SubscribeOrderBook(accountType BinanceAccountType, symbol string) error {
	return b.SubscribeOrderBookWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *BinanceOrderBook) SubscribeOrderBooks(accountType BinanceAccountType, symbols []string) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (b *BinanceOrderBook) SubscribeOrderBookWithCallBack(accountType BinanceAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, []string{symbol}, callback)
}

func (b *BinanceOrderBook) SubscribeOrderBooksWithCallBack(accountType BinanceAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBackAndZeroCopy(accountType, symbols, callback, false)
}

// 批量订阅深度并带上回调
func (b *BinanceOrderBook) SubscribeOrderBooksWithCallBackAndZeroCopy(accountType BinanceAccountType, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	log.Infof("开始订阅增量OrderBook深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBinanceOrderBookBase *binanceOrderBookBase

	switch accountType {
	case BINANCE_SPOT:
		currentBinanceOrderBookBase = b.SpotOrderBook
	case BINANCE_FUTURE:
		currentBinanceOrderBookBase = b.FutureOrderBook
	case BINANCE_SWAP:
		currentBinanceOrderBookBase = b.SwapOrderBook
	default:
		return ErrorAccountType
	}
	//订阅总数超过LEN次，分批订阅
	LEN := currentBinanceOrderBookBase.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := b.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentBinanceOrderBookBase.subscribeBinanceDepthMultipleWithZeroCopy(client, tempSymbols, callback, isZeroCopy)
			if err != nil {
				return err
			}
			currentCount, ok := currentBinanceOrderBookBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("深度%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentBinanceOrderBookBase.subscribeBinanceDepthMultipleWithZeroCopy(client, symbols, callback, isZeroCopy)
		if err != nil {
			return err
		}
	}

	log.Infof("增量OrderBook深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// ReSubscribeOrderBook 强制退订并重订指定 symbol 的增量盘口（含同 multiplex 连接上的 sibling）。
// 会中止该 symbol 上卡住的 initBinanceDepthFunc，清空本地快照后重新缓冲并拉 REST。
// 同一 OrderBook 账户类型上串行执行；组内已在重启/刚重启过则直接返回，避免并行 bump epoch 把新 init 掐死。
func (b *BinanceOrderBook) ReSubscribeOrderBook(accountType BinanceAccountType, symbol string) error {
	if b == nil {
		return fmt.Errorf("binance orderbook not initialized")
	}
	base, err := b.getBaseMapFromAccountType(accountType)
	if err != nil {
		return err
	}
	if base == nil {
		return fmt.Errorf("binance orderbook not initialized")
	}
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return fmt.Errorf("empty symbol")
	}
	if base.closed.Load() {
		return fmt.Errorf("binance orderbook closed")
	}
	log.Warnf("币安深度手动重订阅 accountType=%s symbol=%s", accountType, symbol)
	base.restartBinanceDepthStreamLocked(symbol, true)
	return nil
}

func (b *binanceOrderBookBase) currentDepthInitEpoch(symbol string) int64 {
	if b == nil || b.depthInitEpoch == nil {
		return 0
	}
	v, _ := b.depthInitEpoch.Load(symbol)
	return v
}

func (b *binanceOrderBookBase) bumpDepthInitEpoch(symbols []string) {
	if b == nil || b.depthInitEpoch == nil {
		return
	}
	for _, symbol := range symbols {
		if symbol == "" {
			continue
		}
		v, _ := b.depthInitEpoch.Load(symbol)
		b.depthInitEpoch.Store(symbol, v+1)
	}
}

func (b *binanceOrderBookBase) depthInitAborted(symbol string, epoch int64) bool {
	if b == nil {
		return true
	}
	if b.closed.Load() {
		return true
	}
	return b.currentDepthInitEpoch(symbol) != epoch
}

func (b *binanceOrderBookBase) waitDepthInitIdle(symbols []string) {
	b.waitDepthInitIdleFor(symbols, binanceDepthInitIdleWait)
}

func (b *binanceOrderBookBase) waitDepthInitIdleFor(symbols []string, perSymbol time.Duration) {
	if b == nil || perSymbol <= 0 {
		return
	}
	for _, symbol := range symbols {
		if symbol == "" || b.IsInitActionMu == nil {
			continue
		}
		mu, ok := b.IsInitActionMu.Load(symbol)
		if !ok || mu == nil {
			continue
		}
		deadline := time.Now().Add(perSymbol)
		for {
			if mu.TryLock() {
				mu.Unlock()
				break
			}
			if !time.Now().Before(deadline) {
				log.Warnf("币安深度等待 init 锁超时 symbol=%s", symbol)
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (b *binanceOrderBookBase) restartBinanceDepthStream(symbol string) {
	go b.restartBinanceDepthStreamLocked(symbol, false)
}

func (b *binanceOrderBookBase) restartBinanceDepthStreamLocked(symbol string, clearCache bool) {
	if b == nil {
		return
	}
	b.depthRestartMu.Lock()
	defer b.depthRestartMu.Unlock()
	if b.closed.Load() {
		return
	}
	if b.WsClientMap == nil || b.CallBackMap == nil {
		return
	}
	symbols := b.symbolsOnSameDepthSub(symbol)
	if binanceDepthAnyResubInFlight(b.depthResubInFlight, symbols) {
		return
	}
	nowMilli := time.Now().UnixMilli()
	if binanceDepthGroupRecentlyRestarted(b.depthLastRestartMilli, symbols, nowMilli, binanceDepthRestartDebounce) {
		return
	}
	stampBinanceDepthGroupRestarted(b.depthLastRestartMilli, symbols, nowMilli)
	b.bumpDepthInitEpoch(symbols)
	client, _ := b.WsClientMap.Load(symbol)
	callback, _ := b.CallBackMap.Load(symbol)
	var zeroCopy bool
	if b.depthSubZeroCopy != nil {
		zeroCopy, _ = b.depthSubZeroCopy.Load(symbol)
	}
	for _, s := range symbols {
		if clearCache {
			b.resetBinanceDepthLocalState(s, true)
		} else {
			b.invalidateBinanceDepthReadyKeepSnapshot(s)
		}
	}
	b.waitDepthInitIdle(symbols)
	if b.closed.Load() {
		return
	}
	b.resubscribeBinanceDepthAfterClose(client, symbols, callback, zeroCopy)
	stampBinanceDepthGroupRestarted(b.depthLastRestartMilli, symbols, time.Now().UnixMilli())
}

func (b *binanceOrderBookBase) binanceDepthSubStillActive(sub *mybinanceapi.Subscription[mybinanceapi.WsDepth], symbols []string) bool {
	if sub == nil || b.SubMap == nil {
		return false
	}
	for _, symbol := range symbols {
		if cur, ok := b.SubMap.Load(symbol); ok && cur == sub {
			return true
		}
	}
	return false
}

func (b *binanceOrderBookBase) symbolsOnSameDepthSub(symbol string) []string {
	out := []string{symbol}
	if b.SubMap == nil {
		return out
	}
	sub, ok := b.SubMap.Load(symbol)
	if !ok || sub == nil {
		return out
	}
	seen := map[string]struct{}{symbol: {}}
	b.SubMap.Range(func(s string, other *mybinanceapi.Subscription[mybinanceapi.WsDepth]) bool {
		if other == sub {
			if _, dup := seen[s]; !dup {
				out = append(out, s)
				seen[s] = struct{}{}
			}
		}
		return true
	})
	return out
}

func (b *binanceOrderBookBase) claimDepthResubSymbols(symbols []string) (pending []string, backoff time.Duration) {
	if b.depthResubInFlight == nil || b.depthResubAttempt == nil {
		return nil, 0
	}
	b.depthResubMu.Lock()
	defer b.depthResubMu.Unlock()
	maxAttempt := 0
	for _, symbol := range symbols {
		if symbol == "" {
			continue
		}
		if _, flying := b.depthResubInFlight.Load(symbol); flying {
			continue
		}
		attempt, _ := b.depthResubAttempt.Load(symbol)
		attempt++
		b.depthResubAttempt.Store(symbol, attempt)
		b.depthResubInFlight.Store(symbol, true)
		pending = append(pending, symbol)
		if attempt > maxAttempt {
			maxAttempt = attempt
		}
	}
	if len(pending) == 0 {
		return nil, 0
	}
	return pending, binanceDepthResubBackoffAfterAttempt(maxAttempt)
}

func (b *binanceOrderBookBase) releaseDepthResubInFlight(symbols []string) {
	if b.depthResubInFlight == nil {
		return
	}
	for _, symbol := range symbols {
		b.depthResubInFlight.Delete(symbol)
	}
}

func (b *binanceOrderBookBase) detachBinanceDepthSymbols(symbols []string) []*mybinanceapi.WsStreamClient {
	var clients []*mybinanceapi.WsStreamClient
	seen := map[*mybinanceapi.WsStreamClient]struct{}{}
	for _, symbol := range symbols {
		if c, ok := b.WsClientMap.Load(symbol); ok && c != nil {
			if _, dup := seen[c]; !dup {
				seen[c] = struct{}{}
				clients = append(clients, c)
			}
			if b.WsClientListMap != nil {
				if count, ok := b.WsClientListMap.Load(c); ok && count != nil {
					if n := atomic.AddInt64(count, -1); n < 0 {
						atomic.StoreInt64(count, 0)
					}
				}
			}
		}
		b.WsClientMap.Delete(symbol)
		b.SubMap.Delete(symbol)
	}
	return clients
}

func (b *binanceOrderBookBase) retireBinanceDepthWsClients(clients []*mybinanceapi.WsStreamClient, dropFromPool bool) {
	seen := map[*mybinanceapi.WsStreamClient]struct{}{}
	for _, c := range clients {
		if c == nil {
			continue
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		unused := !binanceDepthWsClientStillUsed(b.WsClientMap, c, nil)
		if b.WsClientListMap != nil && (unused || dropFromPool) {
			b.WsClientListMap.Delete(c)
		}
		if unused {
			client := c
			go func() {
				_ = client.Close()
			}()
		}
	}
}

func (b *binanceOrderBookBase) resubscribeBinanceDepthAfterClose(
	oldClient *mybinanceapi.WsStreamClient,
	symbols []string,
	callback func(depth *Depth, err error),
	isZeroCopy bool,
) {
	if b.closed.Load() || len(symbols) == 0 {
		return
	}
	pending, backoff := b.claimDepthResubSymbols(symbols)
	if len(pending) == 0 {
		return
	}
	if backoff > 0 {
		time.Sleep(backoff)
		if b.closed.Load() {
			b.releaseDepthResubInFlight(pending)
			return
		}
	}

	for _, symbol := range pending {
		b.invalidateBinanceDepthReadyKeepSnapshot(symbol)
	}

	unsubTimedOut := false
	unsubscribed := map[*mybinanceapi.Subscription[mybinanceapi.WsDepth]]struct{}{}
	for _, symbol := range pending {
		sub, ok := b.SubMap.Load(symbol)
		if !ok || sub == nil {
			continue
		}
		if _, done := unsubscribed[sub]; done {
			continue
		}
		unsubscribed[sub] = struct{}{}
		if err := unsubscribeBinanceDepthSubWithTimeout(sub.Unsubscribe, binanceDepthUnsubscribeTimeout); err != nil {
			log.Warnf("币安深度退订失败/超时 symbols=%v err=%v", pending, err)
			if errors.Is(err, errBinanceDepthUnsubscribeTimeout) {
				unsubTimedOut = true
			}
		}
	}

	oldClients := b.detachBinanceDepthSymbols(pending)
	if len(oldClients) == 0 && oldClient != nil {
		oldClients = []*mybinanceapi.WsStreamClient{oldClient}
	}
	b.retireBinanceDepthWsClients(oldClients, unsubTimedOut)

	if b.closed.Load() {
		b.releaseDepthResubInFlight(pending)
		return
	}

	client, err := b.GetCurrentOrNewWsClient(b.AccountType)
	if err != nil {
		log.Errorf("币安深度重订获取连接失败 symbols=%v err=%v", pending, err)
		b.releaseDepthResubInFlight(pending)
		go b.resubscribeBinanceDepthAfterClose(oldClient, pending, callback, isZeroCopy)
		return
	}
	if err = b.subscribeBinanceDepthMultipleWithZeroCopy(client, pending, callback, isZeroCopy); err != nil {
		log.Errorf("币安深度重订失败 symbols=%v err=%v", pending, err)
		if b.WsClientListMap != nil && !binanceDepthWsClientStillUsed(b.WsClientMap, client, binanceDepthSymbolSet(pending)) {
			b.WsClientListMap.Delete(client)
			go func() {
				_ = client.Close()
			}()
		}
		b.releaseDepthResubInFlight(pending)
		go b.resubscribeBinanceDepthAfterClose(client, pending, callback, isZeroCopy)
		return
	}
	b.releaseDepthResubInFlight(pending)
}

func (b *binanceOrderBookBase) Close() {
	if b == nil {
		return
	}
	b.closed.Store(true)
	b.depthRestartMu.Lock()
	defer b.depthRestartMu.Unlock()
	b.BinanceWsClientBase.close()

	b.OrderBookCacheMap.Clear()
	b.OrderBookRBTreeMap.Clear()
	b.OrderBookReadyUpdateIdMap.Clear()
	b.OrderBookMap.Clear()
	b.OrderBookLastUpdateIdMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.IsInitActionMu.Clear()
	b.CallBackMap.Clear()
	b.depthSubZeroCopy.Clear()
	if b.depthResubInFlight != nil {
		b.depthResubInFlight.Clear()
	}
	if b.depthResubAttempt != nil {
		b.depthResubAttempt.Clear()
	}
	if b.depthInitEpoch != nil {
		b.depthInitEpoch.Clear()
	}
	if b.depthLastRestartMilli != nil {
		b.depthLastRestartMilli.Clear()
	}
}

func (b *BinanceOrderBook) Close() {
	if b == nil {
		return
	}
	if b.restQuotaCron != nil {
		b.restQuotaCron.Stop()
		b.restQuotaCron = nil
	}
	b.SpotOrderBook.Close()
	b.FutureOrderBook.Close()
	b.SwapOrderBook.Close()
}
