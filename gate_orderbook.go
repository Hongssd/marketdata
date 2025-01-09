package marketdata

import (
	"context"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mygateapi"
	"github.com/robfig/cron/v3"
)

type GateOrderBook struct {
	parent            *GateMarketData
	SpotOrderBook     *gateOrderBookBase
	FuturesOrderBook  *gateOrderBookBase
	DeliveryOrderBook *gateOrderBookBase
}

type gateOrderBookBase struct {
	parent                    *GateOrderBook
	limitRestCountPerMinute   int64  //每分钟rest最大请求数
	currentRestCount          int64  //当前rest请求数
	uSpeed                    string //深度更新速度
	callBackDepthLevel        int64  //回调深度档位
	callBackDepthTimeoutMilli int64  //回调深度超时时间
	initOrderBookSize         int    //初始OrderBook档位
	GateWsClientBase
	Exchange                  Exchange
	AccountType               GateAccountType
	OrderBookCacheMap         *MySyncMap[string, *MySyncMap[int64, *mygateapi.WsOrderBook]]
	OrderBookRBTreeMap        *MySyncMap[string, *OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookBaseIdMap        *MySyncMap[string, int64]
	WsClientMap               *MySyncMap[string, *mygateapi.WsStreamClient]                                                           //symbol->wsClient
	SubMap                    *MySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsOrderBook]]] //symbol->subscribe
	IsInitActionMu            *MySyncMap[string, *sync.Mutex]                                                                         //symbol->mutex
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]                                                       //symbol->callback
}

// 根据类型获取基础
func (b *GateOrderBook) getBaseMapFromAccountType(accountType GateAccountType) (*gateOrderBookBase, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotOrderBook, nil
	case GATE_FUTURES:
		return b.FuturesOrderBook, nil
	case GATE_DELIVERY:
		return b.DeliveryOrderBook, nil
	}
	return nil, ErrorAccountType
}

// 新建Gate深度基础
func (b *GateOrderBook) newGateOrderBookBase(config GateOrderBookConfigBase) *gateOrderBookBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 10
	}
	if config.PerConnSubNum == 0 {
		config.PerSubMaxLen = 50
	}
	return &gateOrderBookBase{
		Exchange: GATE,
		GateWsClientBase: GateWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mygateapi.WsStreamClient, *int64]()),
		},
		uSpeed:                    config.USpeed,
		limitRestCountPerMinute:   config.LimitRestCountPerMinute,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		initOrderBookSize:         config.InitOrderBookSize,
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, *MySyncMap[int64, *mygateapi.WsOrderBook]]()),
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, *OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookBaseIdMap:        GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mygateapi.WsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsOrderBook]]]()),
		IsInitActionMu:            GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}

}

// 初始化
func (b *GateOrderBook) init() {
	c := cron.New(cron.WithSeconds())
	//每隔1分钟刷新一次请求次数
	_, err := c.AddFunc("0 */1 * * * *", func() {
		atomic.StoreInt64(&b.SpotOrderBook.currentRestCount, 0)
		atomic.StoreInt64(&b.FuturesOrderBook.currentRestCount, 0)
		atomic.StoreInt64(&b.DeliveryOrderBook.currentRestCount, 0)
	})
	if err != nil {
		log.Error(err)
		return
	}
	c.Start()
}

// 获取当前或新建ws客户端
func (b *GateOrderBook) GetCurrentOrNewWsClient(accountType GateAccountType) (*mygateapi.WsStreamClient, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotOrderBook.GetCurrentOrNewWsClient(accountType)
	case GATE_FUTURES:
		return b.FuturesOrderBook.GetCurrentOrNewWsClient(accountType)
	case GATE_DELIVERY:
		return b.DeliveryOrderBook.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

// 封装好的获取深度方法
func (b *GateOrderBook) GetDepth(GateAccountType GateAccountType, symbol string, level int, timeoutMilli int64) (*Depth, error) {
	bmap, err := b.getBaseMapFromAccountType(GateAccountType)
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

// 订阅Gate深度底层执行
func (b *gateOrderBookBase) subscribeGateDepthMultiple(gateWsClient *mygateapi.WsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {

	gateSub, err := gateWsClient.SubscribeOrderBookUpdateMultiple(symbols, b.uSpeed, "100")
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		b.WsClientMap.Store(symbol, gateWsClient)
		b.SubMap.Store(symbol, gateSub)
		b.CallBackMap.Store(symbol, callback)
		//初始化深度锁
		b.IsInitActionMu.Store(symbol, &sync.Mutex{})
	}
	go func() {
		for {
			// log.Info("next gateSub...")
			select {
			case err := <-gateSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-gateSub.ResultChan():
				result := *r.Result
				Symbol := result.Symbol
				//检测深度是否初始化
				_, err := b.checkGateDepthIsReady(Symbol)
				if err != nil {
					//直接存入深度缓存
					b.saveGateDepthCache(result)
					continue
				}

				//判断是否丢包
				if currentLastId, ok := b.OrderBookBaseIdMap.Load(Symbol); ok {
					if result.FirstId > currentLastId+1 {
						//log.Warnf("发生丢包: %s:%s thisFirstId:%d, thisLastId:%d currentLastId:%d", b.AccountType, result.Symbol, result.FirstId, result.LastId, currentLastId)
						//清空相关数据
						b.OrderBookReadyUpdateIdMap.Delete(Symbol)
						b.OrderBookMap.Delete(Symbol)
						b.OrderBookBaseIdMap.Delete(Symbol)
						b.OrderBookCacheMap.Delete(Symbol)
						b.OrderBookRBTreeMap.Delete(Symbol)
						//重新初始化深度
						go func() {
							err := b.initGateDepthFunc(result.Symbol)
							if err != nil {
								log.Error(err)
							}
						}()
						continue
					} else {
						//log.Infof("正常数据包: %s:%s thisFirstId:%d, thisLastId:%d currentLastId:%d", b.AccountType, result.Symbol, result.FirstId, result.LastId, currentLastId)
					}
				}

				//log.Warn(result.LowerU, result.UpperU, result.LastUpdateID)

				//保存至OrderBook
				err = b.saveGateDepthOrderBook(result)
				if err != nil {
					log.Error(err)
					continue
				}

				if callback == nil || b.callBackDepthLevel == 0 {
					continue
				}
				depth, err := b.parent.GetDepth(b.AccountType, Symbol, int(b.callBackDepthLevel), b.callBackDepthTimeoutMilli)
				if err != nil {
					callback(nil, err)
					continue
				}
				depth.UId, depth.PreUId = b.GetUidAndPreUid(result)
				callback(depth, err)
			case <-gateSub.CloseChan():
				log.Info("订阅已关闭: ", gateSub.SubKeys)
				return
			}
		}
	}()

	go func() {
		//log.Info("订阅成功, 开始初始化深度池...")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		// 创建一个新的Group
		g, ctx := errgroup.WithContext(ctx)
		for _, symbol := range symbols {
			symbol := symbol
			g.Go(func() error {
				//初始化深度池
				err = b.initGateDepthFunc(symbol)
				if err != nil {
					log.Error(err)
					return err
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			log.Error(err)
			return
		}
	}()
	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(gateWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(gateWsClient, &initCount)

	}
	atomic.AddInt64(count, currentCount)
	return nil
}

//// 取消订阅Gate深度
//func (b *gateOrderBookBase) UnSubscribeGateDepth(Symbol string) error {
//	gateSub, ok := b.SubMap.Load(Symbol)
//	if !ok {
//		return nil
//	}
//	return gateSub.Unsubscribe()
//}
//
//// 重新订阅Gate深度
//func (b *gateOrderBookBase) ReSubscribeGateDepth(Symbol string) error {
//	err := b.UnSubscribeGateDepth(Symbol)
//	if err != nil {
//		return err
//	}
//	gateWsClient, ok := b.WsClientMap.Load(Symbol)
//	if !ok {
//		err := fmt.Errorf("symbol:%s gateWsClient not found", Symbol)
//		return err
//	}
//	callBack, ok := b.CallBackMap.Load(Symbol)
//	if !ok {
//		err := fmt.Errorf("symbol:%s callBack not found", Symbol)
//		return err
//	}
//	return b.subscribeGateDepthMultiple(gateWsClient, []string{Symbol}, callBack)
//}

// 初始化Gate深度
func (b *gateOrderBookBase) initGateDepthFunc(symbol string) error {
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
	err := b.initGateDepthOrderBook(symbol)
	for err != nil {
		// log.Error(err)
		time.Sleep(time.Second * 1)
		log.Info("重新初始化Gate深度: ", symbol)
		err = b.initGateDepthOrderBook(symbol)
	}
	//初始化完毕，将缓存保存至OrderBook
	return b.saveGateDepthOrderBookFromCache(symbol)
}

// 检测深度是否准备好
func (b *gateOrderBookBase) checkGateDepthIsReady(Symbol string) (int64, error) {
	readyId, isReady := b.OrderBookReadyUpdateIdMap.Load(Symbol)
	if !isReady {
		err := fmt.Errorf("%s 深度未准备好", Symbol)
		return 0, err
	}
	return readyId, nil
}

// 初始化深度
func (b *gateOrderBookBase) initGateDepthOrderBook(Symbol string) error {
	if atomic.LoadInt64(&b.currentRestCount) >= b.limitRestCountPerMinute {
		return fmt.Errorf("Gaterest请求次数超出限制")
	}
	atomic.AddInt64(&b.currentRestCount, 1)
	orderBook, ok := b.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		b.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	switch b.AccountType {
	case GATE_SPOT:
		//重新初始化
		res, err := mygateapi.NewRestClient("", "").PublicRestClient().
			NewPublicRestSpotOrderBook().CurrencyPair(Symbol).Limit(b.initOrderBookSize).WithId(true).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		depth := res.Data
		//d, _ := json.Marshal(depth)
		//log.Warn(string(d))
		//保存至OrderBook
		for _, bid := range depth.Bids {
			if len(bid) != 2 {
				continue
			}
			p, _ := decimal.NewFromString(bid[0])
			q, _ := decimal.NewFromString(bid[1])
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			if len(ask) != 2 {
				continue
			}
			p, _ := decimal.NewFromString(ask[0])
			q, _ := decimal.NewFromString(ask[1])
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
		}
		b.OrderBookBaseIdMap.Store(Symbol, depth.ID)
	case GATE_FUTURES:
		//重新初始化
		res, err := mygateapi.NewRestClient("", "").PublicRestClient().
			NewPublicRestFuturesSettleOrderBook().Settle("usdt").Contract(Symbol).Limit(b.initOrderBookSize).WithId(true).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		depth := res.Data
		//d, _ := json.Marshal(depth)
		//log.Warn(string(d))
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, _ := decimal.NewFromString(bid.P)
			q := decimal.NewFromInt(bid.S)
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, _ := decimal.NewFromString(ask.P)
			q := decimal.NewFromInt(ask.S)
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())

		}
		b.OrderBookBaseIdMap.Store(Symbol, depth.Id)
	case GATE_DELIVERY:
		//重新初始化
		res, err := mygateapi.NewRestClient("", "").PublicRestClient().
			NewPublicRestDeliverySettleOrderBook().Settle("usdt").Contract(Symbol).Limit(b.initOrderBookSize).WithId(true).Do()
		if err != nil {
			log.Error(err)
			return err
		}
		depth := res.Data
		//d, _ := json.Marshal(depth)
		//log.Warn(string(d))
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, _ := decimal.NewFromString(bid.Price)
			q := decimal.NewFromInt(bid.Quantity)
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
		for _, ask := range depth.Asks {
			p, _ := decimal.NewFromString(ask.Price)
			q := decimal.NewFromInt(ask.Quantity)
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
		}
		b.OrderBookBaseIdMap.Store(Symbol, depth.ID)
	}

	//log.Info(b.OrderBookBaseIdMap.Load(Symbol))
	//log.Info(b.OrderBookReadyUpdateIdMap.Load(Symbol))
	return nil
}

// 将Depth缓存保存至OrderBook
func (b *gateOrderBookBase) saveGateDepthOrderBookFromCache(Symbol string) error {

	// readyId, err := b.checkGateDepthIsReady(Symbol)
	// if err != nil {
	// 	log.Error(err)
	// 	return err
	// }
	baseId, ok := b.OrderBookBaseIdMap.Load(Symbol)
	if !ok {
		err := fmt.Errorf("%s baseId not found", Symbol)
		log.Error(err)
		return err
	}
	//log.Info(baseId)

	//读取缓存到OrderBook
	cacheMap, ok := b.OrderBookCacheMap.Load(Symbol)
	if !ok {
		newMap := NewMySyncMap[int64, *mygateapi.WsOrderBook]()
		cacheMap = &newMap
		b.OrderBookCacheMap.Store(Symbol, cacheMap)
	}

	//按照LowerU排序
	var cacheList []mygateapi.WsOrderBook
	cacheMap.Range(func(k int64, v *mygateapi.WsOrderBook) bool {
		cacheList = append(cacheList, *v)
		return true
	})
	sort.Sort(SortGateWsOrderBookSlice(cacheList))

	//log.Warn(cacheList)
	for len(cacheList) == 0 {
		time.Sleep(100 * time.Millisecond)
		cacheMap.Range(func(k int64, v *mygateapi.WsOrderBook) bool {
			cacheList = append(cacheList, *v)
			return true
		})
		sort.Sort(SortGateWsOrderBookSlice(cacheList))
	}

	//若最小的缓存都比当前的baseId大，则等待1秒后需要重新请求rest接口
	for len(cacheList) > 0 && cacheList[0].FirstId > baseId {
		time.Sleep(500 * time.Millisecond)
		err := b.initGateDepthOrderBook(Symbol)
		if err != nil {
			return err
		}
		baseId, _ = b.OrderBookBaseIdMap.Load(Symbol)
		//log.Warn(baseId)
	}

	//若最大的缓存都比当前的baseId小，则等待100毫秒后重新读取缓存
	for len(cacheList) > 0 && cacheList[len(cacheList)-1].LastId < baseId {
		time.Sleep(100 * time.Millisecond)
		cacheList = []mygateapi.WsOrderBook{}
		cacheMap, _ = b.OrderBookCacheMap.Load(Symbol)
		cacheMap.Range(func(k int64, v *mygateapi.WsOrderBook) bool {
			cacheList = append(cacheList, *v)
			return true
		})
		sort.Sort(SortGateWsOrderBookSlice(cacheList))
		//log.Warn(cacheList)
	}

	// log.Info(baseId)
	// log.Info(len(cacheList))
	targetCacheList := []mygateapi.WsOrderBook{}
	for index, v := range cacheList {
		if v.FirstId <= baseId+1 && v.LastId >= baseId+1 {
			err := b.saveGateDepthOrderBook(v)
			if err != nil {
				log.Error(err)
				return err
			}
			targetCacheList = cacheList[index:]
			break
		}
	}
	// log.Info(len(targetCacheList))
	for _, v := range targetCacheList {
		err := b.saveGateDepthOrderBook(v)
		if err != nil {
			log.Error(err)
			return err
		}
		baseId = v.LastId
	}

	if _, ok := b.OrderBookRBTreeMap.Load(Symbol); ok {
		b.OrderBookReadyUpdateIdMap.Store(Symbol, baseId)
	}

	return nil
}

// 将Depth缓存
func (b *gateOrderBookBase) saveGateDepthCache(result mygateapi.WsOrderBook) {
	Symbol := result.Symbol

	cacheMap, ok := b.OrderBookCacheMap.Load(Symbol)
	if !ok {
		newMap := NewMySyncMap[int64, *mygateapi.WsOrderBook]()
		cacheMap = &newMap
		b.OrderBookCacheMap.Store(Symbol, cacheMap)
	}
	cacheMap.Store(result.LastId, &result)
}

// 将Depth保存至OrderBook
func (b *gateOrderBookBase) saveGateDepthOrderBook(result mygateapi.WsOrderBook) error {
	Symbol := result.Symbol

	b.OrderBookBaseIdMap.Store(Symbol, result.LastId)

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
			p, _ := decimal.NewFromString(bid.Price)
			q, _ := decimal.NewFromString(bid.Quantity)
			if q.IsZero() {
				orderBook.RemoveBid(p.InexactFloat64())
				continue
			}
			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ask := range result.Asks {
			p, _ := decimal.NewFromString(ask.Price)
			q, _ := decimal.NewFromString(ask.Quantity)
			if q.IsZero() {
				orderBook.RemoveAsk(p.InexactFloat64())
				continue
			}
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
		}
	}()
	wg.Wait()
	//log.Warn(result.LowerU, result.UpperU, result.LastUpdateID)

	UId, PreUId := b.GetUidAndPreUid(result)
	depth := &Depth{
		UId:         UId,
		PreUId:      PreUId,
		AccountType: string(b.AccountType),
		Exchange:    string(b.Exchange),
		Symbol:      result.Symbol,
		Timestamp:   result.Timestamp + b.parent.parent.GetServerTimeDelta(),
	}
	b.OrderBookMap.Store(Symbol, depth)

	return nil
}

func (b *gateOrderBookBase) GetUidAndPreUid(result mygateapi.WsOrderBook) (int64, int64) {
	UId := result.LastId
	PreUId := result.FirstId - 1
	return UId, PreUId
}

// 订阅深度
func (b *GateOrderBook) SubscribeOrderBook(accountType GateAccountType, symbol string) error {
	return b.SubscribeOrderBookWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *GateOrderBook) SubscribeOrderBooks(accountType GateAccountType, symbols []string) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (b *GateOrderBook) SubscribeOrderBookWithCallBack(accountType GateAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅深度并带上回调
func (b *GateOrderBook) SubscribeOrderBooksWithCallBack(accountType GateAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅增量OrderBook深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentGateOrderBookBase *gateOrderBookBase

	switch accountType {
	case GATE_SPOT:
		currentGateOrderBookBase = b.SpotOrderBook
	case GATE_FUTURES:
		currentGateOrderBookBase = b.FuturesOrderBook
	case GATE_DELIVERY:
		currentGateOrderBookBase = b.DeliveryOrderBook
	default:
		return ErrorAccountType
	}
	//订阅总数超过LEN次，分批订阅
	LEN := currentGateOrderBookBase.perSubMaxLen
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
			err = currentGateOrderBookBase.subscribeGateDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := currentGateOrderBookBase.WsClientListMap.Load(client)
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
		err = currentGateOrderBookBase.subscribeGateDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("增量OrderBook深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *gateOrderBookBase) Close() {
	b.GateWsClientBase.close()

	b.OrderBookCacheMap.Clear()
	b.OrderBookRBTreeMap.Clear()
	b.OrderBookReadyUpdateIdMap.Clear()
	b.OrderBookMap.Clear()
	b.OrderBookBaseIdMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.IsInitActionMu.Clear()
	b.CallBackMap.Clear()

}

func (b *GateOrderBook) Close() {
	b.SpotOrderBook.Close()
	b.FuturesOrderBook.Close()
	b.DeliveryOrderBook.Close()
}
