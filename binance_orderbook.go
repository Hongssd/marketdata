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
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
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

				//判断是否丢包
				if lastLowerU, ok := b.OrderBookLastUpdateIdMap.Load(Symbol); ok {
					if b.AccountType == BINANCE_SPOT {
						if result.UpperU > lastLowerU+1 {
							// log.Warnf("发生丢包: %s:%s U:%d, lu:%d", result.AccountType, result.Symbol, result.UpperU, lastLowerU)
							//清空相关数据
							b.OrderBookReadyUpdateIdMap.Delete(Symbol)
							b.OrderBookMap.Delete(Symbol)
							b.OrderBookLastUpdateIdMap.Delete(Symbol)
							b.OrderBookCacheMap.Delete(Symbol)
							b.OrderBookRBTreeMap.Delete(Symbol)
							//重新初始化深度
							go func() {
								err := b.initBinanceDepthFunc(result.Symbol)
								if err != nil {
									log.Error(err)
								}
							}()
							continue
						} else if result.UpperU < lastLowerU+1 && result.LowerU >= lastLowerU+1 {
							// log.Infof("首个正常数据包: %s:%s U:%d, u:%d lu:%d", result.AccountType, result.Symbol, result.UpperU, result.LowerU, lastLowerU)
						} else {
							// log.Infof("正常数据包: %s:%s U:%d, lu:%d", result.AccountType, result.Symbol, result.UpperU, lastLowerU)
						}
					} else {
						if result.PreU > lastLowerU {
							// log.Warnf("发生丢包: %s:%s preu:%d, lu:%d", result.AccountType, result.Symbol, result.PreU, lastLowerU)

							//清空相关数据
							b.OrderBookReadyUpdateIdMap.Delete(Symbol)
							b.OrderBookMap.Delete(Symbol)
							b.OrderBookLastUpdateIdMap.Delete(Symbol)
							b.OrderBookCacheMap.Delete(Symbol)
							b.OrderBookRBTreeMap.Delete(Symbol)
							//重新初始化深度
							go func() {
								err := b.initBinanceDepthFunc(result.Symbol)
								if err != nil {
									log.Error(err)
								}
							}()
							continue
						} else if result.UpperU <= lastLowerU && result.LowerU >= lastLowerU {
							// log.Infof("首个正常数据包: %s:%s preu:%d, u:%d U:%d lu:%d", result.AccountType, result.Symbol, result.PreU, result.LowerU, result.UpperU, lastLowerU)
						} else {
							// log.Infof("正常数据包: %s:%s preu:%d, lu:%d", result.AccountType, result.Symbol, result.PreU, lastLowerU)
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
	err := b.initBinanceDepthOrderBook(symbol)
	for err != nil {
		// log.Error(err)
		time.Sleep(time.Second * 5)
		log.Info("重新初始化币安深度: ", symbol)
		err = b.initBinanceDepthOrderBook(symbol)
	}
	//初始化完毕，将缓存保存至OrderBook
	return b.saveBinanceDepthOrderBookFromCache(symbol)
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
		//重新初始化
		depth, err := binance.NewFutureRestClient("", "").NewFutureDepth().Symbol(Symbol).Limit(50).Do()
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
		//重新初始化
		depth, err := binance.NewSwapRestClient("", "").NewSwapDepth().Symbol(Symbol).Limit(50).Do()
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

	// log.Info(lastUpdateId)
	// log.Info(len(cacheList))
	targetCacheList := []mybinanceapi.WsDepth{}
	for index, v := range cacheList {
		if b.AccountType != BINANCE_SPOT {
			if v.UpperU <= lastUpdateId && v.LowerU >= lastUpdateId {
				err := b.saveBinanceDepthOrderBook(v)
				if err != nil {
					log.Error(err)
					return err
				}
				targetCacheList = cacheList[index:]
				break
			}
		} else {
			if v.UpperU < lastUpdateId+1 && v.LowerU >= lastUpdateId+1 {
				targetCacheList = cacheList[index:]
				err := b.saveBinanceDepthOrderBook(v)
				if err != nil {
					log.Error(err)
					return err
				}
				break
			}
		}
	}
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
