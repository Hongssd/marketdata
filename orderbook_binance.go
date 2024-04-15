package marketdata

import (
	"fmt"
	"sort"
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
}

type binanceOrderBookBase struct {
	parent                    *BinanceOrderBook
	serverTimeDelta           int64
	limitRestCountPerMinute   int64
	currentRestCount          int64
	perConnSubNum             int64
	uSpeed                    string
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	Exchange                  Exchange
	AccountType               BinanceAccountType
	OrderBookCacheMap         *MySyncMap[string, *MySyncMap[int64, *mybinanceapi.WsDepth]]
	OrderBookRBTreeMap        *MySyncMap[string, *OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientListMap           *MySyncMap[*mybinanceapi.WsStreamClient, int64]                      //wsClinet->subCount
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
	return &binanceOrderBookBase{
		Exchange:                  BINANCE,
		uSpeed:                    config.USpeed,
		limitRestCountPerMinute:   config.LimitRestCountPerMinute,
		perConnSubNum:             config.PerConnSubNum,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, *MySyncMap[int64, *mybinanceapi.WsDepth]]()),
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, *OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientListMap:           GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		IsInitActionMu:            GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}

}

// 初始化
func (b *BinanceOrderBook) init() {
	mybinanceapi.SetLogger(log)
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		b.SpotOrderBook.RefreshDelta()
		b.FutureOrderBook.RefreshDelta()
		b.SwapOrderBook.RefreshDelta()
	}
	refresh()

	//每隔5秒读取一次服务器时间
	_, err := c.AddFunc("*/5 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return
	}

	//每隔1分钟刷新一次请求次数
	_, err = c.AddFunc("0 */1 * * * *", func() {
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

// 获取当前服务器时间差
func (b *BinanceOrderBook) GetServerTimeDelta(accountType BinanceAccountType) int64 {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotOrderBook.serverTimeDelta
	case BINANCE_FUTURE:
		return b.FutureOrderBook.serverTimeDelta
	case BINANCE_SWAP:
		return b.SwapOrderBook.serverTimeDelta
	}
	return 0
}

// 获取当前或新建ws客户端
func (b *BinanceOrderBook) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {
	var WsClientListMap *MySyncMap[*mybinanceapi.WsStreamClient, int64]
	perConnSubNum := int64(0)
	switch accountType {
	case BINANCE_SPOT:
		WsClientListMap = b.SpotOrderBook.WsClientListMap
		perConnSubNum = b.SpotOrderBook.perConnSubNum
	case BINANCE_FUTURE:
		WsClientListMap = b.FutureOrderBook.WsClientListMap
		perConnSubNum = b.FutureOrderBook.perConnSubNum
	case BINANCE_SWAP:
		WsClientListMap = b.SwapOrderBook.WsClientListMap
		perConnSubNum = b.SwapOrderBook.perConnSubNum
	default:
		return nil, ErrorAccountType
	}

	// log.Info(WsClientList)

	var wsClient *mybinanceapi.WsStreamClient
	var err error
	WsClientListMap.Range(func(k *mybinanceapi.WsStreamClient, v int64) bool {
		if v < perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})

	if wsClient == nil {
		//新建链接
		switch accountType {
		case BINANCE_SPOT:
			wsClient = &binance.NewSpotWsStreamClient().WsStreamClient
		case BINANCE_FUTURE:
			wsClient = &binance.NewFutureWsStreamClient().WsStreamClient
		case BINANCE_SWAP:
			wsClient = &binance.NewSwapWsStreamClient().WsStreamClient
		}
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		WsClientListMap.Store(wsClient, 0)
	}
	return wsClient, nil
}

// 订阅深度
func (b *BinanceOrderBook) SubscribeOrderBook(accountType BinanceAccountType, symbol string) error {
	return b.SubscribeOrderBookWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *BinanceOrderBook) SubscribeOrderBooks(accountType BinanceAccountType, symbols []string) error {
	for _, symbol := range symbols {
		err := b.SubscribeOrderBook(accountType, symbol)
		if err != nil {
			return err
		}
	}
	return nil
}

// 订阅深度并带上回调
func (b *BinanceOrderBook) SubscribeOrderBookWithCallBack(accountType BinanceAccountType, symbol string, callback func(depth *Depth, err error)) error {
	client, err := b.GetCurrentOrNewWsClient(accountType)
	if err != nil {
		return err
	}

	switch accountType {
	case BINANCE_SPOT:
		err = b.SpotOrderBook.subscribeBinanceDepth(client, symbol, callback)
	case BINANCE_FUTURE:
		err = b.FutureOrderBook.subscribeBinanceDepth(client, symbol, callback)
	case BINANCE_SWAP:
		err = b.SwapOrderBook.subscribeBinanceDepth(client, symbol, callback)
	}

	return err
}

// 批量订阅深度并带上回调
func (b *BinanceOrderBook) SubscribeOrderBooksWithCallBack(accountType BinanceAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	for _, symbol := range symbols {
		err := b.SubscribeOrderBookWithCallBack(accountType, symbol, callback)
		if err != nil {
			return err
		}
	}
	return nil
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

// 订阅币安深度底层执行
func (b *binanceOrderBookBase) subscribeBinanceDepth(binanceWsClient *mybinanceapi.WsStreamClient, symbol string, callback func(depth *Depth, err error)) error {

	binanceSub, err := binanceWsClient.SubscribeIncrementDepth(symbol, b.uSpeed)
	if err != nil {
		log.Error(err)
		return err
	}
	b.WsClientMap.Store(symbol, binanceWsClient)
	b.SubMap.Store(symbol, binanceSub)
	b.CallBackMap.Store(symbol, callback)
	//初始化深度锁
	b.IsInitActionMu.Store(symbol, &sync.Mutex{})

	go func() {
		for {
			// log.Info("next binanceSub...")
			select {
			case err := <-binanceSub.ErrChan():
				log.Error(err)
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
							go b.initBinanceDepthFunc(result.Symbol)
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

				//保存至OrderBook
				err = b.saveBinanceDepthOrderBook(result)
				if err != nil {
					log.Error(err)
					continue
				}

				if callback == nil || b.callBackDepthLevel == 0 {
					continue
				}
				callback(b.parent.GetDepth(b.AccountType, Symbol, int(b.callBackDepthLevel), b.callBackDepthTimeoutMilli))
			case <-binanceSub.CloseChan():
				log.Info("订阅已关闭: ", binanceSub.Params)
				return
			}
		}
	}()

	//初始化深度池
	err = b.initBinanceDepthFunc(symbol)
	if err != nil {
		log.Error(err)
		return err
	}
	// log.Infof("初始化币安深度成功: %s %s", b.AccountType, symbol)
	count, ok := b.WsClientListMap.Load(binanceWsClient)
	if !ok {
		b.WsClientListMap.Store(binanceWsClient, 1)
	}
	b.WsClientListMap.Store(binanceWsClient, count+1)
	return nil
}

// 取消订阅币安深度
func (b *binanceOrderBookBase) UnSubscribeBinanceDepth(Symbol string) error {
	binanceSub, ok := b.SubMap.Load(Symbol)
	if !ok {
		return nil
	}
	return binanceSub.Unsubscribe()
}

// 重新订阅币安深度
func (b *binanceOrderBookBase) ReSubscribeBinanceDepth(Symbol string) error {
	err := b.UnSubscribeBinanceDepth(Symbol)
	if err != nil {
		return err
	}
	binanceWsClient, ok := b.WsClientMap.Load(Symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s binanceWsClient not found", Symbol)
		return err
	}
	callBack, ok := b.CallBackMap.Load(Symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s callBack not found", Symbol)
		return err
	}
	return b.subscribeBinanceDepth(binanceWsClient, Symbol, callBack)
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
		depth, err := binance.NewSpotRestClient("", "").NewSpotDepth().Symbol(Symbol).Limit(50).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, q, err := bid.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, q, err := ask.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())

		}
		b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
	case BINANCE_FUTURE:
		//重新初始化
		depth, err := binance.NewFutureRestClient("", "").NewFutureDepth().Symbol(Symbol).Limit(50).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, q, err := bid.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, q, err := ask.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())

		}
		b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
	case BINANCE_SWAP:
		//重新初始化
		depth, err := binance.NewSwapRestClient("", "").NewSwapDepth().Symbol(Symbol).Limit(50).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, q, err := bid.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, q, err := ask.ParseDecimal()
			if err != nil {
				log.Error(err)
				return err
			}
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())

		}
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
	// if !unCheck {
	// 	lastLowerU, ok := b.OrderBookLastUpdateIdMap.Load(Symbol)
	// 	if ok {
	// 		if b.AccountType != BINANCE_SPOT {
	// 			if result.PreU != lastLowerU {
	// 				err := fmt.Errorf("%s lastLowerU:%d,preU:%d", Symbol, lastLowerU, result.PreU)
	// 				log.Error(err)
	// 				return nil, err
	// 			}
	// 			log.Warnf("%s lastLowerU:%d,preU:%d", Symbol, lastLowerU, result.PreU)
	// 		} else {
	// 			if result.UpperU != lastLowerU+1 {
	// 				err := fmt.Errorf("%s lastLowerU:%d,UpperU:%d", Symbol, lastLowerU, result.UpperU)
	// 				log.Error(err)
	// 				return nil, err
	// 			}
	// 			log.Warnf("%s lastLowerU:%d,UpperU:%d", Symbol, lastLowerU, result.UpperU)
	// 		}

	// 	}

	// }

	b.OrderBookLastUpdateIdMap.Store(Symbol, result.LowerU)

	orderBook, ok := b.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		b.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, bid := range result.Bids {
			if bid.Quantity == 0 {
				orderBook.RemoveBid(bid.Price)
				continue
			}
			orderBook.PutBid(bid.Price, bid.Quantity)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ask := range result.Asks {
			if ask.Quantity == 0 {
				orderBook.RemoveAsk(ask.Price)
				continue
			}
			orderBook.PutAsk(ask.Price, ask.Quantity)
		}
	}()
	wg.Wait()

	depth := &Depth{
		AccountType: string(b.AccountType),
		Exchange:    string(b.Exchange),
		Symbol:      result.Symbol,
		Timestamp:   result.Timestamp + b.serverTimeDelta,
	}
	b.OrderBookMap.Store(Symbol, depth)

	return nil
}

func (b *binanceOrderBookBase) RefreshDelta() {
	switch b.AccountType {
	case BINANCE_SPOT:
		b.serverTimeDelta = b.parent.parent.spotServerTimeDelta
	case BINANCE_FUTURE:
		b.serverTimeDelta = b.parent.parent.futureServerTimeDelta
	case BINANCE_SWAP:
		b.serverTimeDelta = b.parent.parent.swapServerTimeDelta
	}
}
