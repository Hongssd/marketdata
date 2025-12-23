package marketdata

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mysunxapi"
)

type SunxOrderBook struct {
	parent                     *SunxMarketData
	perConnSubNum              int64
	perSubMaxLen               int
	callBackDepthLevel         int
	callBackDepthTimeoutMilli  int64
	initOrderBookSize          int
	Exchange                   Exchange
	OrderBookRBTreeMap         *MySyncMap[string, OrderBook]
	OrderBookLastVersionIdMap  *MySyncMap[string, int64]
	OrderBookReadyVersionIdMap *MySyncMap[string, int64]
	OrderBookMap               *MySyncMap[string, *Depth]
	WsClientListMap            *MySyncMap[*mysunxapi.PublicWsStreamClient, *int64]
	WsClientMap                *MySyncMap[string, *mysunxapi.PublicWsStreamClient]
	SubMap                     *MySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketDepthHighFreqReq, mysunxapi.WsDepthHighFreq]]
	CallBackMap                *MySyncMap[string, func(depth *Depth, err error)]
	ReSubMuMap                 *MySyncMap[string, *sync.Mutex]
	ReSubWsClientMap           *MySyncMap[*mysunxapi.PublicWsStreamClient, *sync.Mutex]
}

// 新建sunx深度基础
func (sm *SunxMarketData) newSunxOrderBook(config SunxOrderBookConfig) *SunxOrderBook {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 10
	}

	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}

	return &SunxOrderBook{
		parent:                    sm,
		perConnSubNum:             config.PerConnSubNum,
		perSubMaxLen:              config.PerSubMaxLen,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,

		Exchange:                   SUNX,
		OrderBookRBTreeMap:         GetPointer(NewMySyncMap[string, OrderBook]()),
		OrderBookLastVersionIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		OrderBookReadyVersionIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:               GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientListMap:            GetPointer(NewMySyncMap[*mysunxapi.PublicWsStreamClient, *int64]()),
		WsClientMap:                GetPointer(NewMySyncMap[string, *mysunxapi.PublicWsStreamClient]()),
		SubMap:                     GetPointer(NewMySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketDepthHighFreqReq, mysunxapi.WsDepthHighFreq]]()),
		CallBackMap:                GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
		ReSubMuMap:                 GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		ReSubWsClientMap:           GetPointer(NewMySyncMap[*mysunxapi.PublicWsStreamClient, *sync.Mutex]()),
	}
}

func (s *SunxOrderBook) GetDepth(symbol string, level int, timeoutMilli int64) (*Depth, error) {
	depth, ok := s.OrderBookMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s depth not found", symbol)
		return nil, err
	}
	orderBook, ok := s.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s orderBook not found", symbol)
		return nil, err
	}
	newDepth, err := orderBook.LoadToDepth(depth, level)
	if err != nil {
		return nil, err
	}
	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
		err := fmt.Errorf("symbol:%s depth timeout", symbol)
		return newDepth, err
	}
	return newDepth, nil
}

func (s *SunxOrderBook) GetCurrentOrNewWsClient() (*mysunxapi.PublicWsStreamClient, error) {
	return s.parent.GetPublicCurrentOrNewWsClient(s.perConnSubNum, s.WsClientListMap)
}
func (s *SunxOrderBook) SubscribeOrderBook(Symbol string) error {
	return s.SubscribeOrderBookWithCallBack(Symbol, nil)
}
func (s *SunxOrderBook) SubscribeOrderBooks(symbols []string) error {
	return s.SubscribeOrderBooksWithCallBack(symbols, nil)
}
func (s *SunxOrderBook) SubscribeOrderBookWithCallBack(Symbol string, callback func(depth *Depth, err error)) error {
	return s.SubscribeOrderBooksWithCallBack([]string{Symbol}, callback)
}
func (s *SunxOrderBook) SubscribeOrderBooksWithCallBack(symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("Sunx开始订阅增量OrderBook深度，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	LEN := s.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := s.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = s.SubscribeOrderBooks(tempSymbols)
			if err != nil {
				return err
			}
			currentCount, ok := s.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("OrderBook深度分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := s.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = s.subscribeSunxDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SunxOrderBook) subscribeSunxDepthMultiple(sunxWsClient *mysunxapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {
	if _, ok := s.ReSubWsClientMap.Load(sunxWsClient); !ok {
		s.ReSubWsClientMap.Store(sunxWsClient, &sync.Mutex{})
	}
	sunxSub, err := sunxWsClient.SubscribeMarketDepthHighFreq(symbols, []int{s.callBackDepthLevel}, true)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		s.WsClientMap.Store(symbol, sunxWsClient)
		s.SubMap.Store(symbol, sunxSub)
		s.CallBackMap.Store(symbol, callback)
		if _, ok := s.ReSubMuMap.Load(symbol); !ok {
			s.ReSubMuMap.Store(symbol, &sync.Mutex{})
		}
	}

	reSubThis := func(symbol string) {
		if mu, ok := s.ReSubWsClientMap.Load(sunxWsClient); ok {
			mu.Lock()
			defer mu.Unlock()
		} else {
			log.Error("resubscribe wsclient mutex not found")
			return
		}
		if mu, ok := s.ReSubMuMap.Load(symbol); ok {
			if mu.TryLock() {
				defer mu.Unlock()
			} else {
				log.Info("resubscribe symbol:", symbol, " mutex is locked")
				return
			}
		} else {
			log.Error("resubscribe symbol:", symbol, " mutex not found")
			return
		}
		_, err := sunxWsClient.SubscribeMarketDepthHighFreq([]string{symbol}, []int{s.callBackDepthLevel}, false)
		if err != nil {
			log.Error(err)
			return
		}
		s.WsClientMap.Delete(symbol)
		s.SubMap.Delete(symbol)
		s.CallBackMap.Delete(symbol)
		if count, ok := s.WsClientListMap.Load(sunxWsClient); ok {
			atomic.AddInt64(count, -1)
		}
		hasSub := false
		s.SubMap.Range(func(k string, v *mysunxapi.Subscription[mysunxapi.WsMarketDepthHighFreqReq, mysunxapi.WsDepthHighFreq]) bool {
			if v == sunxSub {
				hasSub = true
				return false
			}
			return true
		})
		if !hasSub {
			log.Info("上层订阅已关闭: ", sunxSub.SubReqs)
			sunxSub.CloseChan() <- struct{}{}
		}

		err = s.subscribeSunxDepthMultiple(sunxWsClient, []string{symbol}, callback)
		if err != nil {
			log.Error(err)
			return
		}
	}

	go func() {
		for {
			select {
			case err := <-sunxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-sunxSub.ResultChan():
				split := strings.Split(result.Ch, ".")
				if len(split) < 2 {
					log.Error("subscribe err, r.ch could be empty")
					continue
				}
				Symbol := split[1]
				switch result.Tick.Event {
				case "snapshot":
					//首次接收到推送全量数据，清除其他数据，并直接初始化
					s.OrderBookReadyVersionIdMap.Delete(Symbol)
					s.OrderBookMap.Delete(Symbol)
					s.OrderBookRBTreeMap.Delete(Symbol)
					s.initSunxDepthOrderBook(result)
					s.OrderBookLastVersionIdMap.Store(Symbol, result.Tick.Version)
					// log.Infof("snapshot: %v", result)
				case "update":
					//增量数据更新，需要进行校验
					_, err := s.checkSunxDepthIsReady(Symbol)
					if err != nil {
						//首次全量数据丢失，直接重新订阅
						go reSubThis(Symbol)
						continue
					}
					// log.Infof("update: %v", result)
					err = s.saveSunxDepthOrderBook(result)
					if err != nil {
						log.Error(err)
						//保存增量数据失败，直接重新订阅
						go reSubThis(Symbol)
						continue
					}

					if callback == nil || s.callBackDepthLevel == 0 {
						continue
					}

					depth, err := s.GetDepth(Symbol, int(s.callBackDepthLevel), s.callBackDepthTimeoutMilli)
					if err != nil {
						callback(nil, err)
						continue
					}
					callback(depth, err)
				default:
					//定量深度推送，直接覆盖全部
					s.initAndClearSunxDepthOrderBook(result)
					err = s.saveSunxDepthOrderBook(result)
					if err != nil {
						log.Error(err)
						//保存增量数据失败，直接重新订阅
						go reSubThis(Symbol)
						continue
					}
					if callback == nil || s.callBackDepthLevel == 0 {
						continue
					}
					depth, err := s.GetDepth(Symbol, int(s.callBackDepthLevel), s.callBackDepthTimeoutMilli)
					if err != nil {
						callback(nil, err)
						continue
					}
					callback(depth, err)
				}
			case <-sunxSub.CloseChan():
				log.Info("订阅已关闭: ", sunxSub.SubReqs)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := s.WsClientListMap.Load(sunxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		s.WsClientListMap.Store(sunxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (s *SunxOrderBook) checkSunxDepthIsReady(Symbol string) (int64, error) {
	readyId, isReady := s.OrderBookReadyVersionIdMap.Load(Symbol)
	if !isReady {
		err := fmt.Errorf("%s 深度未准备好", Symbol)
		return 0, err
	}
	return readyId, nil
}

func (s *SunxOrderBook) initSunxDepthOrderBook(result mysunxapi.WsDepthHighFreq) {
	split := strings.Split(result.Ch, ".")
	if len(split) < 2 {
		log.Error("subscribe err, r.ch could be empty")
		return
	}
	Symbol := split[1]
	orderBook, ok := s.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		s.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}

	bidPrices := make([]float64, 0, len(result.Tick.Bids))
	bidQuantities := make([]float64, 0, len(result.Tick.Bids))
	askPrices := make([]float64, 0, len(result.Tick.Asks))
	askQuantities := make([]float64, 0, len(result.Tick.Asks))

	for _, bid := range result.Tick.Bids {
		p := bid.Price
		q := bid.Volume
		bidPrices = append(bidPrices, p)
		bidQuantities = append(bidQuantities, q)
	}

	for _, ask := range result.Tick.Asks {
		p := ask.Price
		q := ask.Volume
		askPrices = append(askPrices, p)
		askQuantities = append(askQuantities, q)
	}

	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)

	s.OrderBookReadyVersionIdMap.Store(Symbol, result.Tick.Version)
	s.OrderBookLastVersionIdMap.Store(Symbol, result.Tick.Version)
}

func (s *SunxOrderBook) initAndClearSunxDepthOrderBook(result mysunxapi.WsDepthHighFreq) {
	split := strings.Split(result.Ch, ".")
	if len(split) < 2 {
		log.Error("subscribe err, r.ch could be empty")
		return
	}
	Symbol := split[1]
	orderBook, ok := s.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		s.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	orderBook.ClearAll()
}

func (s *SunxOrderBook) saveSunxDepthOrderBook(result mysunxapi.WsDepthHighFreq) error {
	split := strings.Split(result.Ch, ".")
	if len(split) < 2 {
		log.Error("subscribe err, r.ch could be empty")
		return nil
	}
	Symbol := split[1]

	preVersionId, ok := s.OrderBookLastVersionIdMap.Load(Symbol)
	if ok {
		if result.Tick.Version > preVersionId {
			//正常推送
		} else if result.Tick.Version == preVersionId {
			//无更新
			return nil
		} else if result.Tick.Version < preVersionId || preVersionId+1 != result.Tick.Version {
			//序列重置
			s.OrderBookReadyVersionIdMap.Delete(Symbol)
			s.OrderBookLastVersionIdMap.Delete(Symbol)
			s.OrderBookMap.Delete(Symbol)
			s.OrderBookRBTreeMap.Delete(Symbol)
			return fmt.Errorf("%s 序列重置，需要重新订阅", Symbol)
		}
	}
	orderBook, ok := s.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		s.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	if orderBook == nil {
		return nil
	}

	bidPrices := make([]float64, 0, len(result.Tick.Bids))
	bidQuantities := make([]float64, 0, len(result.Tick.Bids))
	askPrices := make([]float64, 0, len(result.Tick.Asks))
	askQuantities := make([]float64, 0, len(result.Tick.Asks))

	for _, bid := range result.Tick.Bids {
		p := bid.Price
		q := bid.Volume
		bidPrices = append(bidPrices, p)
		bidQuantities = append(bidQuantities, q)
	}
	for _, ask := range result.Tick.Asks {
		p := ask.Price
		q := ask.Volume
		askPrices = append(askPrices, p)
		askQuantities = append(askQuantities, q)
	}
	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)

	now := time.Now().UnixMilli()
	ts := result.Tick.Ts
	targetTs := ts + s.parent.ServerTimeDelta
	if targetTs > now {
		targetTs = now
	}
	if !ok {
		preVersionId = 0
	}

	depth := &Depth{
		UId:         result.Tick.Version,
		PreUId:      preVersionId,
		AccountType: SUNX_SWAP,
		Exchange:    string(s.Exchange),
		Symbol:      Symbol,
		Timestamp:   targetTs,
	}
	s.OrderBookMap.Store(Symbol, depth)
	s.OrderBookLastVersionIdMap.Store(Symbol, result.Tick.Version)
	return nil
}

func (s *SunxOrderBook) Close() {
	s.WsClientListMap.Range(func(k *mysunxapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})
	s.OrderBookRBTreeMap.Clear()
	s.OrderBookReadyVersionIdMap.Clear()
	s.OrderBookLastVersionIdMap.Clear()
	s.OrderBookMap.Clear()
}
