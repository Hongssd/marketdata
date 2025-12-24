package marketdata

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

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
	level                     string //深度档位
	callBackDepthLevel        int64  //回调深度档位
	callBackDepthTimeoutMilli int64  //回调深度超时时间
	initOrderBookSize         int    //初始OrderBook档位
	GateWsClientBase
	Exchange                  Exchange
	AccountType               GateAccountType
	OrderBookCacheMap         *MySyncMap[string, *MySyncMap[int64, *mygateapi.WsOrderBook]]
	OrderBookRBTreeMap        *MySyncMap[string, OrderBook]
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
		level:                     config.Level,
		limitRestCountPerMinute:   config.LimitRestCountPerMinute,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		initOrderBookSize:         config.InitOrderBookSize,
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, *MySyncMap[int64, *mygateapi.WsOrderBook]]()),
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, OrderBook]()),
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

// 封装好的获取深度方法(高性能视图模式)
func (b *GateOrderBook) ViewDepth(GateAccountType GateAccountType, symbol string, level int, timeoutMilli int64, bizLogic func(*Depth) error) error {
	bmap, err := b.getBaseMapFromAccountType(GateAccountType)
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

// 订阅Gate深度底层执行
func (b *gateOrderBookBase) subscribeGateDepthMultipleWithZeroCopy(gateWsClient *mygateapi.WsStreamClient, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	if b.AccountType == GATE_SPOT || b.AccountType == GATE_DELIVERY {
		b.uSpeed = "100ms"
	}
	gateSub, err := gateWsClient.SubscribeOrderBookUpdateMultiple(symbols, b.uSpeed, b.level)
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
					// log.Infof("缓存数据包: %s:%s thisFirstId:%d, thisLastId:%d \n上[%v]\n下[%v]",
					// 	b.AccountType, result.Symbol, result.FirstId, result.LastId, result.Asks, result.Bids)
					continue
				}

				//判断是否丢包
				if currentLastId, ok := b.OrderBookBaseIdMap.Load(Symbol); ok {
					if result.FirstId > currentLastId+1 {
						// log.Warnf("发生丢包: %s:%s thisFirstId:%d, thisLastId:%d currentLastId:%d", b.AccountType, result.Symbol, result.FirstId, result.LastId, currentLastId)
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
						// log.Infof("正常数据包: %s:%s currentLastId:%d thisFirstId:%d, thisLastId:%d \n上[%v]\n下[%v]",
						// 	b.AccountType, result.Symbol, currentLastId, result.FirstId, result.LastId, result.Asks, result.Bids)
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
			case <-gateSub.CloseChan():
				log.Info("订阅已关闭: ", gateSub.SubKeys)
				return
			}
		}
	}()

	go func() {
		// log.Infof("[%s#%s] 订阅成功, 开始初始化深度池... %v", b.Exchange, b.AccountType, symbols)
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
		log.Error(err)
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
	// var baseId int64
	// var baseDepth interface{}
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

		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))

		for _, bid := range depth.Bids {
			if len(bid) != 2 {
				continue
			}
			p, _ := strconv.ParseFloat(bid[0], 64)
			q, _ := strconv.ParseFloat(bid[1], 64)
			bidPrices = append(bidPrices, p)
			bidQuantities = append(bidQuantities, q)
		}
		for _, ask := range depth.Asks {
			if len(ask) != 2 {
				continue
			}
			p, _ := strconv.ParseFloat(ask[0], 64)
			q, _ := strconv.ParseFloat(ask[1], 64)
			askPrices = append(askPrices, p)
			askQuantities = append(askQuantities, q)
		}
		orderBook.PutBidLevels(bidPrices, bidQuantities)
		orderBook.PutAskLevels(askPrices, askQuantities)

		b.OrderBookBaseIdMap.Store(Symbol, depth.ID)
		// baseId = depth.ID
		// baseDepth = depth
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
		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))

		for _, bid := range depth.Bids {
			p, _ := strconv.ParseFloat(bid.P, 64)
			q := float64(bid.S)
			bidPrices = append(bidPrices, p)
			bidQuantities = append(bidQuantities, q)
		}
		for _, ask := range depth.Asks {
			p, _ := strconv.ParseFloat(ask.P, 64)
			q := float64(ask.S)
			askPrices = append(askPrices, p)
			askQuantities = append(askQuantities, q)

		}
		orderBook.PutBidLevels(bidPrices, bidQuantities)
		orderBook.PutAskLevels(askPrices, askQuantities)
		b.OrderBookBaseIdMap.Store(Symbol, depth.Id)
		// baseId = depth.Id
		// baseDepth = depth
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
		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))
		//保存至OrderBook
		for _, bid := range depth.Bids {
			p, _ := strconv.ParseFloat(bid.Price, 64)
			q := float64(bid.Quantity)
			bidPrices = append(bidPrices, p)
			bidQuantities = append(bidQuantities, q)
		}
		for _, ask := range depth.Asks {
			p, _ := strconv.ParseFloat(ask.Price, 64)
			q := float64(ask.Quantity)
			askPrices = append(askPrices, p)
			askQuantities = append(askQuantities, q)
		}
		b.OrderBookBaseIdMap.Store(Symbol, depth.ID)
		// baseId = depth.ID
		// baseDepth = depth
	}

	// log.Infof("[%s#%s] 初始化深度成功: %s ID[%d]\n深度[%v]", b.Exchange, b.AccountType, Symbol, baseId, baseDepth)

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

	//从Map中重新读取缓存
	reloadCacheList := func() []mygateapi.WsOrderBook {
		var targetCacheList []mygateapi.WsOrderBook
		var cacheList []mygateapi.WsOrderBook
		cacheMap.Range(func(k int64, v *mygateapi.WsOrderBook) bool {
			cacheList = append(cacheList, *v)
			return true
		})
		sort.Sort(SortGateWsOrderBookSlice(cacheList))
		targetCacheList = []mygateapi.WsOrderBook{}
		for index, v := range cacheList {
			if v.FirstId <= baseId+1 && v.LastId >= baseId+1 || v.FirstId > baseId {
				targetCacheList = cacheList[index:]
				break
			}
		}
		return targetCacheList
	}

	targetCacheList := reloadCacheList()

	for len(targetCacheList) == 0 {
		// log.Infof("[%s#%s] 缓存列表为空 等待100毫秒后重新获取缓存列表: %v", b.Exchange, b.AccountType, targetCacheList)
		time.Sleep(100 * time.Millisecond)
		targetCacheList = reloadCacheList()
	}

	ids := []string{}
	for _, v := range targetCacheList {
		ids = append(ids, fmt.Sprintf("%d->%d", v.FirstId, v.LastId))
	}
	// log.Info("targetCacheList: ", ids)

	//若最小的缓存都比当前的baseId大，则等待500毫秒后需要初始化深度
	for targetCacheList[0].FirstId > baseId+1 {

		// log.Infof("[%s#%s] 最小的缓存都比当前的baseId大 等待500毫秒后重新初始化深度: %s ID[%d]", b.Exchange, b.AccountType, Symbol, baseId)
		// oldBaseId := baseId
		time.Sleep(500 * time.Millisecond)
		err := b.initGateDepthOrderBook(Symbol)
		if err != nil {
			return err
		}
		baseId, _ = b.OrderBookBaseIdMap.Load(Symbol)
		// log.Infof("[%s#%s] 重新初始化深度成功: %s ID[%d -> %d]", b.Exchange, b.AccountType, Symbol, oldBaseId, baseId)
		targetCacheList = reloadCacheList()
		for len(targetCacheList) == 0 {
			time.Sleep(100 * time.Millisecond)
			targetCacheList = reloadCacheList()
		}
		ids := []string{}
		for _, v := range targetCacheList {
			ids = append(ids, fmt.Sprintf("%d->%d", v.FirstId, v.LastId))
		}
		// log.Info("重新获取缓存列表:", ids)
	}

	//若最大的缓存都比当前的baseId小，则等待100毫秒后重新读取缓存
	for targetCacheList[len(targetCacheList)-1].LastId < baseId {
		time.Sleep(100 * time.Millisecond)
		targetCacheList = reloadCacheList()
	}
	// //若缓存不连续，则打印错误日志，并将缓存清空
	// preLastId := int64(0)
	// for i := 0; i < len(targetCacheList); i++ {
	// 	if i == 0 {
	// 		preLastId = targetCacheList[i].LastId
	// 		log.Infof("[%s] rest基准id[%d] 首个目标缓存开始id[%d] 结束ID[%d]", Symbol, baseId, targetCacheList[i].FirstId, targetCacheList[i].LastId)
	// 		continue
	// 	}
	// 	if targetCacheList[i].FirstId != preLastId+1 {
	// 		err := fmt.Errorf("[%s]目标缓存不连续: %d, %d", Symbol, targetCacheList[i].FirstId, preLastId+1)
	// 		log.Error(err)
	// 		b.OrderBookCacheMap.Delete(Symbol)
	// 		b.OrderBookRBTreeMap.Delete(Symbol)
	// 		b.OrderBookBaseIdMap.Delete(Symbol)
	// 		b.OrderBookReadyUpdateIdMap.Delete(Symbol)
	// 		b.OrderBookMap.Delete(Symbol)
	// 		break
	// 	}
	// 	preLastId = targetCacheList[i].LastId
	// }
	// log.Infof("[%s]目标缓存连续 rest基准ID[%d] 缓存起始ID[%d->%d] 缓存结束ID:[%d->%d]", Symbol, baseId,
	// 	targetCacheList[0].FirstId, targetCacheList[0].LastId,
	// 	targetCacheList[len(targetCacheList)-1].FirstId, targetCacheList[len(targetCacheList)-1].LastId,
	// )

	// targetCacheList = reloadCacheList()
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

	bidPrices := make([]float64, 0, len(result.Bids))
	bidQuantities := make([]float64, 0, len(result.Bids))
	askPrices := make([]float64, 0, len(result.Asks))
	askQuantities := make([]float64, 0, len(result.Asks))
	for _, bid := range result.Bids {
		p, _ := strconv.ParseFloat(bid.Price, 64)
		q, _ := strconv.ParseFloat(bid.Quantity, 64)
		bidPrices = append(bidPrices, p)
		bidQuantities = append(bidQuantities, q)
	}
	for _, ask := range result.Asks {
		p, _ := strconv.ParseFloat(ask.Price, 64)
		q, _ := strconv.ParseFloat(ask.Quantity, 64)
		askPrices = append(askPrices, p)
		askQuantities = append(askQuantities, q)
	}
	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)
	//log.Warn(result.LowerU, result.UpperU, result.LastUpdateID)

	UId, PreUId := b.GetUidAndPreUid(result)
	now := time.Now().UnixMilli()
	targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta()
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

func (b *GateOrderBook) SubscribeOrderBooksWithCallBack(accountType GateAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBackAndZeroCopy(accountType, symbols, callback, false)
}

// 批量订阅深度并带上回调
func (b *GateOrderBook) SubscribeOrderBooksWithCallBackAndZeroCopy(accountType GateAccountType, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
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
			err = currentGateOrderBookBase.subscribeGateDepthMultipleWithZeroCopy(client, tempSymbols, callback, isZeroCopy)
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
		err = currentGateOrderBookBase.subscribeGateDepthMultipleWithZeroCopy(client, symbols, callback, isZeroCopy)
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
