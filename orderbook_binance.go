package marketdata

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybinanceapi"
	"github.com/Hongssd/myokxapi"
	"github.com/robfig/cron/v3"
)

type BinanceOrderBook struct {
	SpotOrderBook   *binanceOrderBookBase
	FutureOrderBook *binanceOrderBookBase
	SwapOrderBook   *binanceOrderBookBase
}

type binanceOrderBookBase struct {
	serverTimeDelta           int64
	limitRestCountPerMinute   int64
	currentRestCount          int64
	perConnSubNum             int64
	uSpeed                    string
	Exchange                  Exchange
	AccountType               BinanceAccountType
	OrderBookCacheMap         *MySyncMap[string, map[int64]*mybinanceapi.WsDepth]
	OrderBookRBTreeMap        *MySyncMap[string, *OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientList              []*mybinanceapi.WsStreamClient
	WsClientMap               *MySyncMap[string, *mybinanceapi.WsStreamClient]
	SubMap                    *MySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]
	IsInitActionMu            *MySyncMap[string, *sync.Mutex]
	CallBackMap               *MySyncMap[string, func(depth *Depth)]
}


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

// 封装好的获取深度方法
func (b *BinanceOrderBook) getDepth(BinanceAccountType BinanceAccountType, symbol string, level int, timeoutMilli int64) (*Depth, error) {
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
		return nil, err
	}
	return newDepth, nil
}

func (b *BinanceOrderBook) newBinanceOrderBookBase(config BinanceOrderBookConfigBase) *binanceOrderBookBase {
	return &binanceOrderBookBase{
		Exchange:                  BINANCE,
		uSpeed:                    config.USpeed,
		limitRestCountPerMinute:   config.LimitRestCountPerMinute,
		perConnSubNum:             config.PerConnSubNum,
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, map[int64]*mybinanceapi.WsDepth]()),
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, *OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		IsInitActionMu:            GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth)]()),
	}

}



// 初始化
func (b *BinanceOrderBook) init() {

	mybinanceapi.SetLogger(log)
	myokxapi.SetLogger(log)

	c := cron.New(cron.WithSeconds())
	refresh := func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			err := b.RefreshDelta(BINANCE_SPOT)
			if err != nil {
				log.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			err := b.RefreshDelta(BINANCE_SPOT)
			if err != nil {
				log.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			err := b.RefreshDelta(BINANCE_SPOT)
			if err != nil {
				log.Error(err)
			}
		}()
		wg.Wait()
	}
	refresh()

	//每隔1秒更新一次服务器时间
	_, err := c.AddFunc("*/1 * * * * *", refresh)
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

func (b *BinanceOrderBook) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {
	var WsClientList *[]*mybinanceapi.WsStreamClient
	perConnSubNum := 0
	switch accountType {
	case BINANCE_SPOT:
		WsClientList = &b.SpotOrderBook.WsClientList
		perConnSubNum = int(b.SpotOrderBook.perConnSubNum)
	case BINANCE_FUTURE:
		WsClientList = &b.FutureOrderBook.WsClientList
		perConnSubNum = int(b.FutureOrderBook.perConnSubNum)
	case BINANCE_SWAP:
		WsClientList = &b.SwapOrderBook.WsClientList
		perConnSubNum = int(b.SwapOrderBook.perConnSubNum)
	default:
		return nil, ErrorAccountType
	}

	var wsClient *mybinanceapi.WsStreamClient
	var err error
	for _, v := range *WsClientList {
		currentSubList, nerr := wsClient.CurrentSubscribeList()
		if nerr != nil {
			err = nerr
			break
		}
		if len(currentSubList) >= perConnSubNum {
			wsClient = v
			err = nil
			break
		}
	}
	if wsClient == nil && err == nil {
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
		*WsClientList = append(*WsClientList, wsClient)
	}
	return wsClient, nil
}

func (b *BinanceOrderBook) SubscribeDepth(accountType BinanceAccountType, symbol string, callback func(depth *Depth)) error {
	client, err := b.GetCurrentOrNewWsClient(accountType)
	if err != nil {
		return err
	}

	switch accountType {
	case BINANCE_SPOT:
		err = b.SpotOrderBook.SubscribeBinanceDepth(client, symbol, callback)
	case BINANCE_FUTURE:
		err = b.FutureOrderBook.SubscribeBinanceDepth(client, symbol, callback)
	case BINANCE_SWAP:
		err = b.SwapOrderBook.SubscribeBinanceDepth(client, symbol, callback)
	}

	return err
}
func (b *BinanceOrderBook) SubscribeDepths(accountType BinanceAccountType, symbols []string, callback func(depth *Depth)) error {
	for _, symbol := range symbols {
		err := b.SubscribeDepth(accountType, symbol, callback)
		if err != nil {
			return err
		}
	}
	return nil
}

// 订阅币安深度
func (b *binanceOrderBookBase) SubscribeBinanceDepth(binanceWsClient *mybinanceapi.WsStreamClient, symbol string, callback func(depth *Depth)) error {

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
				//保存至OrderBook
				depth, err := b.saveBinanceDepthOrderBook(result, false)
				if err != nil {
					//保存失败，清空相关数据并重新初始化深度，不需要重新订阅
					//清空相关数据
					b.OrderBookReadyUpdateIdMap.Delete(Symbol)
					b.OrderBookMap.Delete(Symbol)
					b.OrderBookLastUpdateIdMap.Delete(Symbol)
					b.OrderBookCacheMap.Delete(Symbol)
					b.OrderBookRBTreeMap.Delete(Symbol)

					//重新初始化深度
					go b.initBinanceDepthFunc(result.Symbol)
					continue
				}
				callback(depth)
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
	log.Info("初始化币安深度成功: ", symbol)
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
	return b.SubscribeBinanceDepth(binanceWsClient, Symbol, callBack)
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
			b.OrderBookReadyUpdateIdMap.Store(Symbol, depth.LastUpdateId)
			b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
		}

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
			b.OrderBookReadyUpdateIdMap.Store(Symbol, depth.LastUpdateId)
			b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
		}

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
			b.OrderBookReadyUpdateIdMap.Store(Symbol, depth.LastUpdateId)
			b.OrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)
		}
	}

	return nil
}

// 将Depth缓存保存至OrderBook
func (b *binanceOrderBookBase) saveBinanceDepthOrderBookFromCache(Symbol string) error {

	readyId, err := b.checkBinanceDepthIsReady(Symbol)
	if err != nil {
		log.Error(err)
		return err
	}

	//读取缓存到OrderBook
	cacheMap, ok := b.OrderBookCacheMap.Load(Symbol)
	if !ok {
		cacheMap = map[int64]*mybinanceapi.WsDepth{}
		b.OrderBookCacheMap.Store(Symbol, cacheMap)
	}

	//按照LowerU排序
	var cacheList []mybinanceapi.WsDepth
	for _, v := range cacheMap {
		cacheList = append(cacheList, *v)
	}
	sort.Sort(SortBinanceWsDepthSlice(cacheList))

	targetCacheList := []mybinanceapi.WsDepth{}
	for index, v := range cacheList {
		if v.UpperU <= readyId && v.LowerU >= readyId {
			// log.Warn("v:", v)
			_, err := b.saveBinanceDepthOrderBook(v, true)
			if err != nil {
				log.Error(err)
				return err
			}
			targetCacheList = cacheList[index+1:]
		}
	}

	for _, v := range targetCacheList {
		_, err := b.saveBinanceDepthOrderBook(v, false)
		if err != nil {
			// log.Error(err)
			return err
		}
	}

	return nil
}

// 将Depth缓存
func (b *binanceOrderBookBase) saveBinanceDepthCache(result mybinanceapi.WsDepth) {
	Symbol := result.Symbol
	cacheMap, ok := b.OrderBookCacheMap.Load(Symbol)
	if !ok {
		cacheMap = map[int64]*mybinanceapi.WsDepth{}
		b.OrderBookCacheMap.Store(Symbol, cacheMap)
	}
	cacheMap[result.LowerU] = &result
}

// 将Depth保存至OrderBook
func (b *binanceOrderBookBase) saveBinanceDepthOrderBook(result mybinanceapi.WsDepth, unCheck bool) (*Depth, error) {
	Symbol := result.Symbol
	if !unCheck {
		lastLowerU, ok := b.OrderBookLastUpdateIdMap.Load(Symbol)
		if ok {
			if result.PreU != lastLowerU {
				err := fmt.Errorf("%s lastLowerU:%d,preU:%d", Symbol, lastLowerU, result.PreU)
				// log.Error(err)
				return nil, err
			}
		}
		// log.Warnf("%s lastLowerU:%d,preU:%d", Symbol, lastLowerU, result.PreU)
	}

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

	return depth, nil
}

func (b *BinanceOrderBook) RefreshDelta(accountType BinanceAccountType) error {
	switch accountType {
	case BINANCE_SPOT:
		res, err := binance.NewSpotRestClient("", "").NewServerTime().Do()
		if err != nil {
			return err
		}
		b.SpotOrderBook.serverTimeDelta = time.Now().UnixMilli() - res.ServerTime
	case BINANCE_FUTURE:
		res, err := binance.NewFutureRestClient("", "").NewServerTime().Do()
		if err != nil {
			return err
		}
		b.FutureOrderBook.serverTimeDelta = time.Now().UnixMilli() - res.ServerTime
	case BINANCE_SWAP:
		res, err := binance.NewSwapRestClient("", "").NewServerTime().Do()
		if err != nil {
			return err
		}
		b.SwapOrderBook.serverTimeDelta = time.Now().UnixMilli() - res.ServerTime
	}

	return nil
}
