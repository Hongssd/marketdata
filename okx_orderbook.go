package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myokxapi"
	"github.com/shopspring/decimal"
)

type OkxOrderBook struct {
	parent                    *OkxMarketData
	perConnSubNum             int64
	perSubMaxLen              int
	wsBooksType               myokxapi.WsBooksType
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	Exchange                  Exchange
	OrderBookRBTreeMap        *MySyncMap[string, *OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientListMap           *MySyncMap[*myokxapi.PublicWsStreamClient, *int64]
	WsClientMap               *MySyncMap[string, *myokxapi.PublicWsStreamClient]
	SubMap                    *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsBooks]]
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]
	ReSubMuMap                *MySyncMap[string, *sync.Mutex]
	ReSubWsClientMap          *MySyncMap[*myokxapi.PublicWsStreamClient, *sync.Mutex]
}

func (om *OkxMarketData) newOkxOrderBook(config OkxOrderBookConfig) *OkxOrderBook {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 10
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	o := &OkxOrderBook{
		parent:                    om,
		perConnSubNum:             config.PerConnSubNum,
		perSubMaxLen:              config.PerSubMaxLen,
		wsBooksType:               config.WsBooksType,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,

		Exchange:                  OKX,
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, *OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientListMap:           GetPointer(NewMySyncMap[*myokxapi.PublicWsStreamClient, *int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *myokxapi.PublicWsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsBooks]]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
		ReSubMuMap:                GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		ReSubWsClientMap:          GetPointer(NewMySyncMap[*myokxapi.PublicWsStreamClient, *sync.Mutex]()),
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

func (o *OkxOrderBook) GetCurrentOrNewWsClient() (*myokxapi.PublicWsStreamClient, error) {
	return o.parent.GetPublicCurrentOrNewWsClient(o.perConnSubNum, o.WsClientListMap)
}
func (o *OkxOrderBook) SubscribeOrderBook(Symbol string) error {
	return o.SubscribeOrderBookWithCallBack(Symbol, nil)
}
func (o *OkxOrderBook) SubscribeOrderBooks(symbols []string) error {
	return o.SubscribeOrderBooksWithCallBack(symbols, nil)
}

func (o *OkxOrderBook) SubscribeOrderBookWithCallBack(Symbol string, callback func(depth *Depth, err error)) error {
	return o.SubscribeOrderBooksWithCallBack([]string{Symbol}, callback)
}
func (o *OkxOrderBook) SubscribeOrderBooksWithCallBack(symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("OKX开始订阅增量OrderBook深度，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	//订阅总数超过LEN次，分批订阅
	LEN := o.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := o.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = o.subscribeOkxDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("OrderBook深度分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = o.subscribeOkxDepthMultiple(client, symbols, callback)
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
func (o *OkxOrderBook) subscribeOkxDepth(okxWsClient *myokxapi.PublicWsStreamClient, symbol string, callback func(depth *Depth, err error)) error {
	return o.subscribeOkxDepthMultiple(okxWsClient, []string{symbol}, callback)
}

// 订阅OKX深度
func (o *OkxOrderBook) subscribeOkxDepthMultiple(okxWsClient *myokxapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {

	if _, ok := o.ReSubWsClientMap.Load(okxWsClient); !ok {
		o.ReSubWsClientMap.Store(okxWsClient, &sync.Mutex{})
	}
	okxSub, err := okxWsClient.SubscribeBooksMultiple(symbols, o.wsBooksType)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		o.WsClientMap.Store(symbol, okxWsClient)
		o.SubMap.Store(symbol, okxSub)
		o.CallBackMap.Store(symbol, callback)
		if _, ok := o.ReSubMuMap.Load(symbol); !ok {
			o.ReSubMuMap.Store(symbol, &sync.Mutex{})
		}
	}

	//重新订阅
	reSubThis := func(symbol string) {
		if mu, ok := o.ReSubWsClientMap.Load(okxWsClient); ok {
			mu.Lock()
			defer mu.Unlock()
		} else {
			//log.Error("resubscribe wsclient mutex not found")
			return
		}
		if mu, ok := o.ReSubMuMap.Load(symbol); ok {
			if mu.TryLock() {
				defer mu.Unlock()
			} else {
				//log.Info("resubscribe symbol:", symbol, " mutex is locked")
				return
			}
		} else {
			//log.Error("resubscribe symbol:", symbol, " mutex not found")
			return
		}
		err := okxWsClient.UnSubscribeBooks(symbol, o.wsBooksType)
		for err != nil && strings.Contains(err.Error(), "websocket is busy") {
			time.Sleep(1000 * time.Millisecond)
			err = okxWsClient.UnSubscribeBooks(symbol, o.wsBooksType)
		}
		if err != nil {
			log.Error(err)
			return
		}
		o.WsClientMap.Delete(symbol)
		o.SubMap.Delete(symbol)
		o.CallBackMap.Delete(symbol)
		if count, ok := o.WsClientListMap.Load(okxWsClient); ok {
			atomic.AddInt64(count, -1)
		}
		//判断上层sub对应的symbol是否为空，如果为空则关闭上层sub
		hasSub := false
		o.SubMap.Range(func(k string, v *myokxapi.Subscription[myokxapi.WsBooks]) bool {
			if v == okxSub {
				hasSub = true
				return false
			}
			return true
		})

		if !hasSub {
			log.Info("上层订阅已关闭: ", okxSub.Args)
			okxSub.CloseChan() <- struct{}{}
		}

		err = o.subscribeOkxDepthMultiple(okxWsClient, []string{symbol}, callback)
		if err != nil {
			log.Error(err)
			return
		}
	}
	go func() {
		for {
			select {
			case err := <-okxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-okxSub.ResultChan():
				Symbol := result.InstId
				switch result.Action {
				case "snapshot":
					//首次接收到推送全量数据，清除其他数据，并直接初始化
					o.OrderBookReadyUpdateIdMap.Delete(Symbol)
					o.OrderBookMap.Delete(Symbol)
					o.OrderBookLastUpdateIdMap.Delete(Symbol)
					o.OrderBookRBTreeMap.Delete(Symbol)
					o.initOkxDepthOrderBook(result)
				case "update":
					//增量数据更新，需要进行校验
					_, err := o.checkOkxDepthIsReady(Symbol)
					if err != nil {
						//首次全量数据丢失，直接重新订阅
						go reSubThis(Symbol)
						continue
					}

					err = o.saveOkxDepthOrderBook(result)
					if err != nil {
						log.Error(err)
						//保存增量数据失败，直接重新订阅
						go reSubThis(Symbol)
						continue
					}

					if callback == nil || o.callBackDepthLevel == 0 {
						continue
					}
					depth, err := o.GetDepth(Symbol, int(o.callBackDepthLevel), o.callBackDepthTimeoutMilli)
					if err != nil {
						callback(nil, err)
						continue
					}
					depth.UId = result.SeqId
					depth.PreUId = result.PrevSeqId
					callback(depth, err)
				}

			case <-okxSub.CloseChan():
				log.Info("订阅已关闭: ", okxSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := o.WsClientListMap.Load(okxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		o.WsClientListMap.Store(okxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
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
	//log.Infof("初始化深度池%s:", Symbol)
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
		//增量推送1（正常更新）：prevSeqId = 10，seqId = 15
		//增量推送2（无更新）：prevSeqId = 15，seqId = 15
		//增量推送3（序列重置）：prevSeqId = 15，seqId = 3
		//增量推送4（正常更新）：prevSeqId = 3，seqId = 5
		if result.SeqId > result.PrevSeqId {
			//正常推送
		} else if result.SeqId == result.PrevSeqId {
			//无更新
			return nil
		} else if result.SeqId < result.PrevSeqId {
			//序列重置
			log.Warnf("%s seqId reset %d to %d", Symbol, result.PrevSeqId, result.SeqId)
		}
		_ = lastSeqId
		//
		//if result.PrevSeqId != lastSeqId {
		//	err := fmt.Errorf("%s lastSeqId:%d,PrevSeqId:%d", Symbol, lastSeqId, result.PrevSeqId)
		//	o.OrderBookLastUpdateIdMap.Store(Symbol, result.SeqId)
		//	return err
		//}
	}
	o.OrderBookLastUpdateIdMap.Store(Symbol, result.SeqId)
	orderBook, ok := o.OrderBookRBTreeMap.Load(Symbol)
	if !ok || orderBook == nil {
		orderBook = NewOrderBook()
		o.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}

	if orderBook == nil {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, bid := range result.Bids {
			p, _ := decimal.NewFromString(bid.Price)
			q, _ := decimal.NewFromString(bid.Quantity)
			if q.IsZero() {
				if orderBook == nil {
					return
				}
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
				if orderBook == nil {
					return
				}
				orderBook.RemoveAsk(p.InexactFloat64())
				continue
			}
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
		}
	}()
	wg.Wait()

	if okx_common == nil {
		okx_common = (&okxCommon{}).InitCommon()
	}
	ts, _ := strconv.ParseInt(result.Ts, 10, 64)
	depth := &Depth{
		UId:         result.SeqId,
		PreUId:      result.PrevSeqId,
		AccountType: okx_common.GetAccountTypeFromSymbol(result.InstId),
		Exchange:    string(o.Exchange),
		Symbol:      result.InstId,
		Timestamp:   ts + o.parent.serverTimeDelta,
	}
	o.OrderBookMap.Store(Symbol, depth)
	return nil
}

func (o *OkxOrderBook) Close() {
	o.WsClientListMap.Range(func(k *myokxapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})
	o.OrderBookRBTreeMap.Clear()
	o.OrderBookReadyUpdateIdMap.Clear()
	o.OrderBookMap.Clear()
	o.OrderBookLastUpdateIdMap.Clear()
	o.WsClientListMap.Clear()
	o.WsClientMap.Clear()
	o.SubMap.Clear()
	o.CallBackMap.Clear()
	o.ReSubMuMap.Clear()
}
