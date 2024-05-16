package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mybybitapi"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BybitOrderBook struct {
	parent           *BybitMarketData
	SpotOrderBook    *bybitOrderBookBase
	LinearOrderBook  *bybitOrderBookBase
	InverseOrderBook *bybitOrderBookBase
}

type bybitOrderBookBase struct {
	parent                    *BybitOrderBook
	limitRestCountPerMinute   int64
	currentRestCount          int64
	level                     string
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	initOrderBookSize         int
	BybitWsClientBase
	Exchange                  Exchange
	AccountType               BybitAccountType
	OrderBookRBTreeMap        *MySyncMap[string, *OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientMap               *MySyncMap[string, *mybybitapi.PublicWsStreamClient]             //symbol->wsClient
	SubMap                    *MySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsDepth]] //symbol->subscribe
	IsInitActionMu            *MySyncMap[string, *sync.Mutex]                                  //symbol->mutex
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]                //symbol->callback
	ReSubMuMap                *MySyncMap[string, *sync.Mutex]
	ReSubWsClientMuMap        *MySyncMap[*mybybitapi.PublicWsStreamClient, *sync.Mutex]
}

// 根据类型获取基础
func (b *BybitOrderBook) getBaseMapFromAccountType(accountType BybitAccountType) (*bybitOrderBookBase, error) {
	switch accountType {
	case BYBIT_SPOT:
		return b.SpotOrderBook, nil
	case BYBIT_LINEAR:
		return b.LinearOrderBook, nil
	case BYBIT_INVERSE:
		return b.InverseOrderBook, nil
	}
	return nil, ErrorAccountType
}

// 新建Bybit深度基础
func (b *BybitOrderBook) newBybitOrderBookBase(config BybitOrderBookConfigBase) *bybitOrderBookBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 10
	}
	if config.PerConnSubNum == 0 {
		config.PerSubMaxLen = 50
	}
	return &bybitOrderBookBase{
		Exchange: BYBIT,
		BybitWsClientBase: BybitWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybybitapi.PublicWsStreamClient, *int64]()),
		},
		level:                     config.Level,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, *OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mybybitapi.PublicWsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsDepth]]()),
		IsInitActionMu:            GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
		ReSubMuMap:                GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		ReSubWsClientMuMap:        GetPointer(NewMySyncMap[*mybybitapi.PublicWsStreamClient, *sync.Mutex]()),
	}

}

// 初始化
func (b *BybitOrderBook) init() {

}

// 获取当前或新建ws客户端
func (b *BybitOrderBook) GetCurrentOrNewWsClient(accountType BybitAccountType) (*mybybitapi.PublicWsStreamClient, error) {
	switch accountType {
	case BYBIT_SPOT:
		return b.SpotOrderBook.GetCurrentOrNewWsClient(accountType)
	case BYBIT_LINEAR:
		return b.LinearOrderBook.GetCurrentOrNewWsClient(accountType)
	case BYBIT_INVERSE:
		return b.InverseOrderBook.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

// 封装好的获取深度方法
func (b *BybitOrderBook) GetDepth(BybitAccountType BybitAccountType, symbol string, level int, timeoutMilli int64) (*Depth, error) {
	bmap, err := b.getBaseMapFromAccountType(BybitAccountType)
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

// 订阅Bybit深度底层执行
func (b *bybitOrderBookBase) subscribeBybitDepthMultiple(bybitWsClient *mybybitapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {

	if _, ok := b.ReSubWsClientMuMap.Load(bybitWsClient); !ok {
		b.ReSubWsClientMuMap.Store(bybitWsClient, &sync.Mutex{})
	}

	bybitSub, err := bybitWsClient.SubscribeDepthMultiple(symbols, b.level)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		b.WsClientMap.Store(symbol, bybitWsClient)
		b.SubMap.Store(symbol, bybitSub)
		b.CallBackMap.Store(symbol, callback)
		//初始化深度锁
		b.IsInitActionMu.Store(symbol, &sync.Mutex{})
		if _, ok := b.ReSubMuMap.Load(symbol); !ok {
			b.ReSubMuMap.Store(symbol, &sync.Mutex{})
		}
	}
	//重新订阅
	reSubThis := func(symbol string) {
		if mu, ok := b.ReSubWsClientMuMap.Load(bybitWsClient); ok {
			mu.Lock()
			defer mu.Unlock()
		} else {
			//log.Error("resubscribe wsClient mutex not found")
			return
		}
		if mu, ok := b.ReSubMuMap.Load(symbol); ok {
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
		err := bybitWsClient.UnSubscribeDepth(symbol, b.level)
		for err != nil && strings.Contains(err.Error(), "websocket is busy") {
			time.Sleep(5000 * time.Millisecond)
			err = bybitWsClient.UnSubscribeDepth(symbol, b.level)
		}
		if err != nil {
			log.Error(err)
			return
		}
		b.WsClientMap.Delete(symbol)
		b.SubMap.Delete(symbol)
		b.CallBackMap.Delete(symbol)
		if count, ok := b.WsClientListMap.Load(bybitWsClient); ok {
			atomic.AddInt64(count, -1)
		}
		//判断上层sub对应的symbol是否为空，如果为空则关闭上层sub
		hasSub := false
		b.SubMap.Range(func(k string, v *mybybitapi.Subscription[mybybitapi.WsDepth]) bool {
			if v == bybitSub {
				hasSub = true
				return false
			}
			return true
		})

		if !hasSub {
			log.Info("上层订阅已关闭: ", bybitSub.Args)
			bybitSub.CloseChan() <- struct{}{}
		}

		err = b.subscribeBybitDepthMultiple(bybitWsClient, []string{symbol}, callback)
		if err != nil {
			log.Error(err)
			return
		}
	}
	go func() {
		for {
			select {
			case err := <-bybitSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-bybitSub.ResultChan():
				Symbol := result.Symbol
				//log.Infof("%s接收到推送数据: %s", result.Symbol, result.Type)
				switch result.Type {
				case "snapshot":

					//首次接收到推送全量数据，清除其他数据，并直接初始化
					b.OrderBookReadyUpdateIdMap.Delete(Symbol)
					b.OrderBookMap.Delete(Symbol)
					b.OrderBookLastUpdateIdMap.Delete(Symbol)
					b.OrderBookRBTreeMap.Delete(Symbol)
					b.initBybitDepthOrderBook(result)
				case "delta":
					//增量数据更新，需要进行校验
					_, err := b.checkBybitDepthIsReady(Symbol)
					if err != nil {
						//log.Error(err)
						//首次全量数据丢失，直接重新订阅
						go reSubThis(Symbol)
						continue
					}

					err = b.saveBybitDepthOrderBook(result)
					if err != nil {
						log.Error(err)
						//保存增量数据失败，直接重新订阅
						go reSubThis(Symbol)
						continue
					}

					if callback == nil || b.callBackDepthLevel == 0 {
						continue
					}
					callback(b.parent.GetDepth(b.AccountType, Symbol, int(b.callBackDepthLevel), b.callBackDepthTimeoutMilli))
				}
			case <-bybitSub.CloseChan():
				log.Info("订阅已关闭: ", bybitSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(bybitWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(bybitWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

// 检测深度是否准备好
func (b *bybitOrderBookBase) checkBybitDepthIsReady(Symbol string) (int64, error) {
	readyId, isReady := b.OrderBookReadyUpdateIdMap.Load(Symbol)
	if !isReady {
		err := fmt.Errorf("%s 深度未准备好", Symbol)
		return 0, err
	}
	return readyId, nil
}

func (b *bybitOrderBookBase) initBybitDepthOrderBook(result mybybitapi.WsDepth) {
	Symbol := result.Symbol
	//log.Infof("初始化深度池%s:", Symbol)
	orderBook := NewOrderBook()
	b.OrderBookRBTreeMap.Store(Symbol, orderBook)
	//保存至OrderBook
	for _, bid := range result.Bids {
		orderBook.PutBid(bid.Float64Result())
	}
	for _, ask := range result.Asks {
		orderBook.PutAsk(ask.Float64Result())
	}

	b.OrderBookReadyUpdateIdMap.Store(Symbol, result.U)
	b.OrderBookLastUpdateIdMap.Store(Symbol, result.U)
}

// 将Depth保存至OrderBook
func (b *bybitOrderBookBase) saveBybitDepthOrderBook(result mybybitapi.WsDepth) error {
	Symbol := result.Symbol
	lastUId, ok := b.OrderBookLastUpdateIdMap.Load(Symbol)
	if ok {
		if result.U-1 != lastUId {
			err := fmt.Errorf("%s lastUId:%d,CurrentId:%d", Symbol, lastUId, result.U)
			return err
		}
	}

	b.OrderBookLastUpdateIdMap.Store(Symbol, result.U)

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
			p, q := bid.DecimalResult()
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
			p, q := ask.DecimalResult()
			if q.IsZero() {
				orderBook.RemoveAsk(p.InexactFloat64())
				continue
			}
			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
		}
	}()
	wg.Wait()

	ts := result.Ts
	depth := &Depth{
		AccountType: b.AccountType.String(),
		Exchange:    b.Exchange.String(),
		Symbol:      result.Symbol,
		Timestamp:   ts + b.parent.parent.ServerTimeDelta,
	}
	b.OrderBookMap.Store(Symbol, depth)
	return nil
}

// 订阅深度
func (b *BybitOrderBook) SubscribeOrderBook(accountType BybitAccountType, symbol string) error {
	return b.SubscribeOrderBookWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *BybitOrderBook) SubscribeOrderBooks(accountType BybitAccountType, symbols []string) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (b *BybitOrderBook) SubscribeOrderBookWithCallBack(accountType BybitAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅深度并带上回调
func (b *BybitOrderBook) SubscribeOrderBooksWithCallBack(accountType BybitAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅增量OrderBook深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBybitOrderBookBase *bybitOrderBookBase

	switch accountType {
	case BYBIT_SPOT:
		currentBybitOrderBookBase = b.SpotOrderBook
	case BYBIT_LINEAR:
		currentBybitOrderBookBase = b.LinearOrderBook
	case BYBIT_INVERSE:
		currentBybitOrderBookBase = b.InverseOrderBook
	default:
		return ErrorAccountType
	}
	//订阅总数超过LEN次，分批订阅
	LEN := currentBybitOrderBookBase.perSubMaxLen
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
			err = currentBybitOrderBookBase.subscribeBybitDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := currentBybitOrderBookBase.WsClientListMap.Load(client)
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
		err = currentBybitOrderBookBase.subscribeBybitDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("增量OrderBook深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *bybitOrderBookBase) Close() {
	b.BybitWsClientBase.close()

	b.OrderBookRBTreeMap.Clear()
	b.OrderBookReadyUpdateIdMap.Clear()
	b.OrderBookMap.Clear()
	b.OrderBookLastUpdateIdMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.IsInitActionMu.Clear()
	b.CallBackMap.Clear()
	b.ReSubMuMap.Clear()
	b.ReSubWsClientMuMap.Clear()

}

func (b *BybitOrderBook) Close() {
	b.SpotOrderBook.Close()
	b.LinearOrderBook.Close()
	b.InverseOrderBook.Close()
}
