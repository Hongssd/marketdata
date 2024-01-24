package marketdata

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Hongssd/myokxapi"
	"github.com/robfig/cron/v3"
	"github.com/shopspring/decimal"
)

type OkxOrderBook struct {
	parent                    *OkxMarketData
	serverTimeDelta           int64
	perConnSubNum             int64
	wsBooksType               myokxapi.WsBooksType
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	Exchange                  Exchange
	OrderBookRBTreeMap        *MySyncMap[string, *OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientListMap           *MySyncMap[*myokxapi.PublicWsStreamClient, int64]
	WsClientMap               *MySyncMap[string, *myokxapi.PublicWsStreamClient]
	SubMap                    *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsBooks]]
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]
}

func (om *OkxMarketData) newOkxOrderBook(config OkxOrderBookConfig) *OkxOrderBook {
	o := &OkxOrderBook{
		parent:                    om,
		serverTimeDelta:           0,
		perConnSubNum:             config.PerConnSubNum,
		wsBooksType:               config.WsBooksType,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		Exchange:                  OKX,
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, *OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientListMap:           GetPointer(NewMySyncMap[*myokxapi.PublicWsStreamClient, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *myokxapi.PublicWsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsBooks]]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
	return o
}

func (o *OkxOrderBook) GetDepth(symbol string, level int, timeoutMilli int64) (*Depth, error) {
	depth, ok := o.OrderBookMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s depth not found", symbol)
		// log.Error(err)
		return nil, err
	}
	orderBook, ok := o.OrderBookRBTreeMap.Load(symbol)
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

func (o *OkxOrderBook) init() {

	myokxapi.SetLogger(log)

	c := cron.New(cron.WithSeconds())
	refresh := func() {
		err := o.RefreshDelta()
		if err != nil {
			log.Error(err)
		}
	}
	refresh()

	//每隔1秒更新一次服务器时间
	_, err := c.AddFunc("*/1 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return
	}

	c.Start()
}

func (o *OkxOrderBook) GetServerTimeDelta() int64 {
	return o.serverTimeDelta
}

func (o *OkxOrderBook) GetCurrentOrNewWsClient() (*myokxapi.PublicWsStreamClient, error) {

	var wsClient *myokxapi.PublicWsStreamClient
	var err error
	o.WsClientListMap.Range(func(k *myokxapi.PublicWsStreamClient, v int64) bool {
		if v < o.perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient == nil {
		//新建链接
		wsClient = okx.NewPublicWsStreamClient()
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		err = wsClient.Login(okx.NewRestClient(o.parent.APIKey, o.parent.SecretKey, o.parent.Passphrase))
		if err != nil {
			log.Error(err)
			return nil, err
		}
		log.Info("ws登录成功")
		o.WsClientListMap.Store(wsClient, 0)
	}
	return wsClient, nil
}
func (o *OkxOrderBook) SubscribeDepth(Symbol string) error {
	return o.SubscribeDepthWithCallBack(Symbol, nil)
}
func (o *OkxOrderBook) SubscribeDepthWithCallBack(Symbol string, callback func(depth *Depth, err error)) error {
	client, err := o.GetCurrentOrNewWsClient()
	if err != nil {
		return err
	}
	return o.subscribeOkxDepth(client, Symbol, callback)
}
func (o *OkxOrderBook) SubscribeDepthsWithCallBack(symbols []string, callback func(depth *Depth, err error)) error {
	for _, symbol := range symbols {
		err := o.SubscribeDepthWithCallBack(symbol, callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OkxOrderBook) SubscribeDepths(symbols []string) error {
	for _, symbol := range symbols {
		err := o.SubscribeDepth(symbol)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OkxOrderBook) DepthContractSizeToQuantity(depth *Depth, contractSize float64) *Depth {
	for _, bid := range depth.Bids {
		bid.Quantity = decimal.NewFromFloat(bid.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
	}
	for _, ask := range depth.Asks {
		ask.Quantity = decimal.NewFromFloat(ask.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
	}
	return depth
}

// 订阅OKX深度
func (o *OkxOrderBook) subscribeOkxDepth(okxWsClient *myokxapi.PublicWsStreamClient, Symbol string, callback func(depth *Depth, err error)) error {

	okxSub, err := okxWsClient.SubscribeBooks(Symbol, o.wsBooksType)
	if err != nil {
		log.Error(err)
		return err
	}
	o.WsClientMap.Store(Symbol, okxWsClient)
	o.SubMap.Store(Symbol, okxSub)
	o.CallBackMap.Store(Symbol, callback)
	go func() {
		for {
			select {
			case err := <-okxSub.ErrChan():
				log.Error(err)
			case result := <-okxSub.ResultChan():
				Symbol := result.InstId
				switch result.Action {
				case "snapshot":
					//首次接收到推送全量数据，清除其他数据，并直接初始化
					o.OrderBookReadyUpdateIdMap.Delete(Symbol)
					o.OrderBookMap.Delete(Symbol)
					o.OrderBookLastUpdateIdMap.Delete(Symbol)
					o.OrderBookRBTreeMap.Delete(Symbol)

					o.initOkxDepthFunc(result)
				case "update":
					//增量数据更新，需要进行校验
					_, err := o.checkOkxDepthIsReady(Symbol)
					if err != nil {
						//首次全量数据丢失，直接重新订阅
						go func() {
							err := o.ReSubscribeOkxDepth(Symbol)
							for err != nil {
								log.Error(err)
								time.Sleep(1000 * time.Millisecond)
								err = o.ReSubscribeOkxDepth(Symbol)
							}
						}()
						continue
					}

					err = o.saveOkxDepthOrderBook(result)
					if err != nil {
						//保存增量数据失败，直接重新订阅
						go func() {
							err := o.ReSubscribeOkxDepth(Symbol)
							for err != nil {
								log.Error(err)
								time.Sleep(1000 * time.Millisecond)
								err = o.ReSubscribeOkxDepth(Symbol)
							}
						}()
						continue
					}

					if callback == nil || o.callBackDepthLevel == 0 {
						continue
					}

					callback(o.GetDepth(Symbol, int(o.callBackDepthLevel), o.callBackDepthTimeoutMilli))
				}

			case <-okxSub.CloseChan():
				log.Info("订阅已关闭: ", okxSub.Args)
				return
			}
		}
	}()

	count, ok := o.WsClientListMap.Load(okxWsClient)
	if !ok {
		o.WsClientListMap.Store(okxWsClient, 1)
	}
	o.WsClientListMap.Store(okxWsClient, count+1)
	return nil
}

// 取消订阅OKX深度
func (o *OkxOrderBook) UnSubscribeOkxDepth(Symbol string) error {
	okxSub, ok := o.SubMap.Load(Symbol)
	if !ok {
		return nil
	}
	return okxSub.Unsubscribe()
}

// 重新订阅OKX深度
func (o *OkxOrderBook) ReSubscribeOkxDepth(Symbol string) error {
	err := o.UnSubscribeOkxDepth(Symbol)
	if err != nil {
		return err
	}
	okxWsClient, ok := o.WsClientMap.Load(Symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s okxWsClient not found", Symbol)
		return err
	}
	callBack, ok := o.CallBackMap.Load(Symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s callBack not found", Symbol)
		return err
	}
	return o.subscribeOkxDepth(okxWsClient, Symbol, callBack)
}

func (o *OkxOrderBook) initOkxDepthFunc(result myokxapi.WsBooks) {
	o.initOkxDepthOrderBook(result)
}

// 检测深度是否准备好

func (o *OkxOrderBook) checkOkxDepthIsReady(Symbol string) (int64, error) {
	readyId, isReady := o.OrderBookReadyUpdateIdMap.Load(Symbol)
	if !isReady {
		err := fmt.Errorf("%s 深度未准备好", Symbol)
		return 0, err
	}
	return readyId, nil
}

func (o *OkxOrderBook) initOkxDepthOrderBook(result myokxapi.WsBooks) {
	Symbol := result.InstId

	orderBook, ok := o.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		o.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	//保存至OrderBook
	for _, bid := range result.Bids {
		p, _ := decimal.NewFromString(bid.Price)
		q, _ := decimal.NewFromString(bid.Quantity)
		orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
	}
	for _, ask := range result.Asks {
		p, _ := decimal.NewFromString(ask.Price)
		q, _ := decimal.NewFromString(ask.Quantity)
		orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
	}

	o.OrderBookReadyUpdateIdMap.Store(Symbol, result.SeqId)
	o.OrderBookLastUpdateIdMap.Store(Symbol, result.SeqId)
}

// 将Depth保存至OrderBook
func (o *OkxOrderBook) saveOkxDepthOrderBook(result myokxapi.WsBooks) error {
	Symbol := result.InstId

	lastSeqId, ok := o.OrderBookLastUpdateIdMap.Load(Symbol)
	if ok {
		if result.PrevSeqId != lastSeqId {
			err := fmt.Errorf("%s lastSeqId:%d,PrevSeqId:%d", Symbol, lastSeqId, result.PrevSeqId)
			return err
		}
	}

	o.OrderBookLastUpdateIdMap.Store(Symbol, result.SeqId)

	orderBook, ok := o.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		o.OrderBookRBTreeMap.Store(Symbol, orderBook)
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

	ts, _ := strconv.ParseInt(result.Ts, 10, 64)
	depth := &Depth{
		AccountType: result.InstType,
		Exchange:    string(o.Exchange),
		Symbol:      result.InstId,
		Timestamp:   ts + o.serverTimeDelta,
	}
	o.OrderBookMap.Store(Symbol, depth)
	return nil
}

func (o *OkxOrderBook) RefreshDelta() error {
	res, err := okx.NewRestClient("", "", "").PublicRestClient().NewPublicRestPublicTime().Do()
	if err != nil {
		return err
	}
	serverTime, err := strconv.ParseInt(res.Data[0].Ts, 10, 64)
	if err != nil {
		return err
	}
	o.serverTimeDelta = time.Now().UnixMilli() - serverTime
	return nil
}
