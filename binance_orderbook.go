package marketdata

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Hongssd/mybinanceapi"
	"github.com/robfig/cron/v3"
)

type BinanceOrderBook struct {
	parent          *BinanceMarketData
	SpotOrderBook   *binanceOrderBookBase
	FutureOrderBook *binanceOrderBookBase
	SwapOrderBook   *binanceOrderBookBase
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
	}

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
		//初始化深度锁
		b.IsInitActionMu.Store(symbol, &sync.Mutex{})
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
				log.Info("订阅已关闭: ", binanceSub.Params)
				return
			}
		}
	}()

	log.Info("订阅成功, 开始初始化深度池...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 创建一个新的Group
	g, ctx := errgroup.WithContext(ctx)
	for _, symbol := range symbols {
		symbol := symbol
		g.Go(func() error {
			//初始化深度池
			err = b.initBinanceDepthFunc(symbol)
			if err != nil {
				log.Error(err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Error(err)
		return err
	}

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(binanceWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(binanceWsClient, &initCount)

	}
	atomic.AddInt64(count, currentCount)

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

// 初始化币安深度
func (b *binanceOrderBookBase) initBinanceDepthFunc(symbol string) error {
	mu, ok := b.IsInitActionMu.Load(symbol)
	if !ok {
		mu = &sync.Mutex{}
		b.IsInitActionMu.Store(symbol, mu)
	}
	if mu.TryLock() {
		defer mu.Unlock()
	} else {
		return nil
	}

snapshotLoop:
	for {
		err := b.initBinanceDepthOrderBook(symbol)
		if err != nil {
			time.Sleep(time.Second * 5)
			log.Info("重新初始化币安深度: ", symbol)
			continue
		}

		// 同快照上等待可桥接增量。低活跃盘口可能长时间无推送（empty_cache），
		// 官方对 snapshot 落后于首个缓冲事件（cache_ahead）是保留缓冲并重取快照。
		waitCount := 0
		for {
			err = b.saveBinanceDepthOrderBookFromCache(symbol)
			if err == nil {
				return nil
			}

			var bridgeErr *binanceDepthBridgeError
			if !errors.As(err, &bridgeErr) {
				log.Error(err)
				b.resetBinanceDepthLocalState(symbol, true)
				time.Sleep(time.Second * 5)
				log.Info("重新初始化币安深度(桥接失败): ", symbol)
				continue snapshotLoop
			}

			clearCache := shouldClearDepthCacheOnBridgeMiss(bridgeErr.Reason)
			switch bridgeErr.Reason {
			case binanceDepthBridgeMissEmptyCache, binanceDepthBridgeMissCacheBehind:
				// 同快照一直等：不清缓存、不重拉 REST（避免低活跃盘口空转）
				waitCount++
				if waitCount == 1 || waitCount%20 == 0 {
					log.Infof("%s depth bridge waiting reason=%s lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d waitCount=%d",
						bridgeErr.Symbol, bridgeErr.Reason, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
						bridgeErr.MinUpperU, bridgeErr.MaxLowerU, waitCount)
				}
				time.Sleep(500 * time.Millisecond)
				continue
			case binanceDepthBridgeMissCacheAhead:
				// 快照过旧：保留已缓冲事件，立刻重取 REST
				log.Errorf("%s depth bridge failed reason=%s lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d clearCache=%v",
					bridgeErr.Symbol, bridgeErr.Reason, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
					bridgeErr.MinUpperU, bridgeErr.MaxLowerU, clearCache)
				b.resetBinanceDepthLocalState(symbol, clearCache)
				continue snapshotLoop
			default:
				// no_covering 等：清缓存后重同步
				log.Errorf("%s depth bridge failed reason=%s lastUpdateId=%d cacheLen=%d minUpperU=%d maxLowerU=%d clearCache=%v",
					bridgeErr.Symbol, bridgeErr.Reason, bridgeErr.LastUpdateId, bridgeErr.CacheLen,
					bridgeErr.MinUpperU, bridgeErr.MaxLowerU, clearCache)
				b.resetBinanceDepthLocalState(symbol, clearCache)
				time.Sleep(time.Second * 5)
				log.Info("重新初始化币安深度(桥接失败): ", symbol)
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
func (b *binanceOrderBookBase) initBinanceDepthOrderBook(Symbol string) error {
	if atomic.LoadInt64(&b.currentRestCount) >= b.limitRestCountPerMinute {
		return fmt.Errorf("币安rest请求次数超出限制")
	}
	atomic.AddInt64(&b.currentRestCount, 1)
	orderBook, ok := b.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		b.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	switch b.AccountType {
	case BINANCE_SPOT:
		//重新初始化
		depth, err := binance.NewSpotRestClient("", "").NewSpotDepth().Symbol(Symbol).Limit(b.initOrderBookSize).Do()
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
		//重新初始化（官方文档推荐 limit=1000）
		depth, err := binance.NewFutureRestClient("", "").NewFutureDepth().Symbol(Symbol).Limit(1000).Do()
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
		//重新初始化（与 U 本位一致，使用更深快照便于本地维护）
		depth, err := binance.NewSwapRestClient("", "").NewSwapDepth().Symbol(Symbol).Limit(1000).Do()
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

	targetCacheList := cacheList[bridgeIdx:]
	// log.Info(len(targetCacheList))
	for _, v := range targetCacheList {
		err := b.saveBinanceDepthOrderBook(v)
		if err != nil {
			log.Error(err)
			return err
		}
		lastUpdateId = v.LowerU
	}

	b.OrderBookReadyUpdateIdMap.Store(Symbol, lastUpdateId)

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
	cacheMap.Store(result.LowerU, &result)
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
	switch reason {
	case binanceDepthBridgeMissEmptyCache, binanceDepthBridgeMissCacheBehind, binanceDepthBridgeMissCacheAhead:
		// empty/behind：继续等增量；ahead：官方要求保留缓冲并重取快照
		return false
	default:
		// no_covering：中间空洞，丢弃后重新缓冲
		return true
	}
}

func binanceDepthBridgeMissWorthWaiting(reason binanceDepthBridgeMissReason) bool {
	return reason == binanceDepthBridgeMissCacheBehind || reason == binanceDepthBridgeMissEmptyCache
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

func (b *binanceOrderBookBase) Close() {
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

}

func (b *BinanceOrderBook) Close() {
	b.SpotOrderBook.Close()
	b.FutureOrderBook.Close()
	b.SwapOrderBook.Close()
}
