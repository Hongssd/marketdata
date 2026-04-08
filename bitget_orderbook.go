package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybitgetapi"
	"github.com/shopspring/decimal"
)

type BitgetOrderBook struct {
	parent                 *BitgetMarketData
	SpotOrderBook          *bitgetOrderBookBase
	UsdtFuturesOrderBook   *bitgetOrderBookBase
	CoinFuturesOrderBook   *bitgetOrderBookBase
	UsdcFuturesOrderBook   *bitgetOrderBookBase
}

type bitgetOrderBookBase struct {
	parent *BitgetOrderBook
	BitgetWsClientBase
	wsBooksType               mybitgetapi.WsBooksType
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	Exchange                  Exchange
	AccountType               BitgetAccountType
	InstType                  mybitgetapi.InstType
	OrderBookRBTreeMap        *MySyncMap[string, OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientMap               *MySyncMap[string, *mybitgetapi.PublicWsStreamClient]
	SubMap                    *MySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsBooks]]
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]
	ReSubMuMap                *MySyncMap[string, *sync.Mutex]
	ReSubWsClientMap          *MySyncMap[*mybitgetapi.PublicWsStreamClient, *sync.Mutex]
}

func normalizeBitgetOrderBookConfigBase(c *BitgetOrderBookConfigBase) {
	if c.PerConnSubNum == 0 {
		c.PerConnSubNum = 10
	}
	if c.PerSubMaxLen == 0 {
		c.PerSubMaxLen = 20
	}
	if c.WsBooksType == "" {
		c.WsBooksType = mybitgetapi.WS_BOOKS_ALL
	}
}

func (bm *BitgetMarketData) newBitgetOrderBook(config BitgetOrderBookConfig) *BitgetOrderBook {
	normalizeBitgetOrderBookConfigBase(&config.SpotConfig)
	normalizeBitgetOrderBookConfigBase(&config.UsdtFuturesConfig)
	normalizeBitgetOrderBookConfigBase(&config.CoinFuturesConfig)
	normalizeBitgetOrderBookConfigBase(&config.UsdcFuturesConfig)
	b := &BitgetOrderBook{parent: bm}
	b.SpotOrderBook = b.newBitgetOrderBookBase(config.SpotConfig, BITGET_SPOT)
	b.SpotOrderBook.parent = b
	b.UsdtFuturesOrderBook = b.newBitgetOrderBookBase(config.UsdtFuturesConfig, BITGET_USDT_FUTURES)
	b.UsdtFuturesOrderBook.parent = b
	b.CoinFuturesOrderBook = b.newBitgetOrderBookBase(config.CoinFuturesConfig, BITGET_COIN_FUTURES)
	b.CoinFuturesOrderBook.parent = b
	b.UsdcFuturesOrderBook = b.newBitgetOrderBookBase(config.UsdcFuturesConfig, BITGET_USDC_FUTURES)
	b.UsdcFuturesOrderBook.parent = b
	return b
}

func (b *BitgetOrderBook) newBitgetOrderBookBase(config BitgetOrderBookConfigBase, accountType BitgetAccountType) *bitgetOrderBookBase {
	instType, _ := BitgetAccountTypeToInstType(accountType)
	return &bitgetOrderBookBase{
		BitgetWsClientBase: BitgetWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybitgetapi.PublicWsStreamClient, *int64]()),
		},
		wsBooksType:               config.WsBooksType,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		Exchange:                  BITGET,
		AccountType:               accountType,
		InstType:                  instType,
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mybitgetapi.PublicWsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsBooks]]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
		ReSubMuMap:                GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		ReSubWsClientMap:          GetPointer(NewMySyncMap[*mybitgetapi.PublicWsStreamClient, *sync.Mutex]()),
	}
}

func (b *BitgetOrderBook) getBase(accountType BitgetAccountType) (*bitgetOrderBookBase, error) {
	switch accountType {
	case BITGET_SPOT:
		return b.SpotOrderBook, nil
	case BITGET_USDT_FUTURES:
		return b.UsdtFuturesOrderBook, nil
	case BITGET_COIN_FUTURES:
		return b.CoinFuturesOrderBook, nil
	case BITGET_USDC_FUTURES:
		return b.UsdcFuturesOrderBook, nil
	default:
		return nil, ErrorAccountType
	}
}

func (b *BitgetOrderBook) GetDepth(accountType BitgetAccountType, symbol string, level int, timeoutMilli int64) (*Depth, error) {
	o, err := b.getBase(accountType)
	if err != nil {
		return nil, err
	}
	return o.GetDepth(symbol, level, timeoutMilli)
}

func (o *bitgetOrderBookBase) GetDepth(symbol string, level int, timeoutMilli int64) (*Depth, error) {
	depth, ok := o.OrderBookMap.Load(symbol)
	if !ok {
		return nil, fmt.Errorf("symbol:%s depth not found", symbol)
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
	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
		return newDepth, fmt.Errorf("symbol:%s depth timeout", symbol)
	}
	return newDepth, nil
}

func (b *BitgetOrderBook) ViewDepth(accountType BitgetAccountType, symbol string, level int, timeoutMilli int64, bizLogic func(*Depth) error) error {
	o, err := b.getBase(accountType)
	if err != nil {
		return err
	}
	return o.ViewDepth(symbol, level, timeoutMilli, bizLogic)
}

func (o *bitgetOrderBookBase) ViewDepth(symbol string, level int, timeoutMilli int64, bizLogic func(*Depth) error) error {
	depth, ok := o.OrderBookMap.Load(symbol)
	if !ok {
		return fmt.Errorf("symbol:%s depth not found", symbol)
	}
	orderBook, ok := o.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
		log.Error(err)
		return err
	}
	return orderBook.ViewDepth(depth, level, func(d *Depth) error {
		if timeoutMilli > 0 && time.Now().UnixMilli()-d.Timestamp > timeoutMilli {
			return fmt.Errorf("symbol:%s depth timeout", symbol)
		}
		return bizLogic(d)
	})
}

func (o *bitgetOrderBookBase) GetCurrentOrNewWsClient() (*mybitgetapi.PublicWsStreamClient, error) {
	return o.BitgetWsClientBase.GetCurrentOrNewWsClient()
}

func (b *BitgetOrderBook) DepthContractSizeToQuantity(accountType BitgetAccountType, depth *Depth, contractSize float64) (*Depth, error) {
	o, err := b.getBase(accountType)
	if err != nil {
		return nil, err
	}
	return o.DepthContractSizeToQuantity(depth, contractSize), nil
}

func (o *bitgetOrderBookBase) DepthContractSizeToQuantity(depth *Depth, contractSize float64) *Depth {
	for _, bid := range depth.Bids {
		bid.Quantity = decimal.NewFromFloat(bid.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
	}
	for _, ask := range depth.Asks {
		ask.Quantity = decimal.NewFromFloat(ask.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
	}
	return depth
}

func (b *BitgetOrderBook) SubscribeOrderBook(accountType BitgetAccountType, symbol string) error {
	return b.SubscribeOrderBookWithCallBack(accountType, symbol, nil)
}

func (b *BitgetOrderBook) SubscribeOrderBooks(accountType BitgetAccountType, symbols []string) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, symbols, nil)
}

func (b *BitgetOrderBook) SubscribeOrderBookWithCallBack(accountType BitgetAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBack(accountType, []string{symbol}, callback)
}

func (b *BitgetOrderBook) SubscribeOrderBooksWithCallBack(accountType BitgetAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	return b.SubscribeOrderBooksWithCallBackAndZeroCopy(accountType, symbols, callback, false)
}

func (b *BitgetOrderBook) SubscribeOrderBooksWithCallBackAndZeroCopy(accountType BitgetAccountType, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	o, err := b.getBase(accountType)
	if err != nil {
		return err
	}
	log.Infof("Bitget 开始订阅增量 OrderBook，accountType:%s，交易对数:%d", accountType, len(symbols))
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
			if err = o.subscribeBitgetDepthMultipleWithZeroCopy(client, tempSymbols, callback, isZeroCopy); err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Bitget OrderBook 分批订阅成功，交易对:%v，当前链接订阅数:%d，等待1秒...", tempSymbols, *currentCount)
			time.Sleep(time.Second)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		if err = o.subscribeBitgetDepthMultipleWithZeroCopy(client, symbols, callback, isZeroCopy); err != nil {
			return err
		}
	}
	return nil
}

func (o *bitgetOrderBookBase) subscribeBitgetDepthMultipleWithZeroCopy(bitgetWsClient *mybitgetapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	if _, ok := o.ReSubWsClientMap.Load(bitgetWsClient); !ok {
		o.ReSubWsClientMap.Store(bitgetWsClient, &sync.Mutex{})
	}
	sub, err := bitgetWsClient.SubscribeBooksMultiple(o.InstType, symbols, o.wsBooksType)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		o.WsClientMap.Store(symbol, bitgetWsClient)
		o.SubMap.Store(symbol, sub)
		o.CallBackMap.Store(symbol, callback)
		if _, ok := o.ReSubMuMap.Load(symbol); !ok {
			o.ReSubMuMap.Store(symbol, &sync.Mutex{})
		}
	}

	reSubThis := func(symbol string) {
		if mu, ok := o.ReSubWsClientMap.Load(bitgetWsClient); ok {
			mu.Lock()
			defer mu.Unlock()
		} else {
			return
		}
		if mu, ok := o.ReSubMuMap.Load(symbol); ok {
			if mu.TryLock() {
				defer mu.Unlock()
			} else {
				return
			}
		} else {
			return
		}
		err := bitgetWsClient.UnSubscribeBooks(o.InstType, symbol, o.wsBooksType)
		for err != nil && strings.Contains(err.Error(), "websocket is busy") {
			time.Sleep(time.Second)
			err = bitgetWsClient.UnSubscribeBooks(o.InstType, symbol, o.wsBooksType)
		}
		if err != nil {
			log.Error(err)
			return
		}
		o.WsClientMap.Delete(symbol)
		o.SubMap.Delete(symbol)
		o.CallBackMap.Delete(symbol)
		if count, ok := o.WsClientListMap.Load(bitgetWsClient); ok {
			atomic.AddInt64(count, -1)
		}
		hasSub := false
		o.SubMap.Range(func(k string, v *mybitgetapi.Subscription[mybitgetapi.WsBooks]) bool {
			if v == sub {
				hasSub = true
				return false
			}
			return true
		})
		if !hasSub {
			log.Info("上层订阅已关闭: ", sub.Args)
			sub.CloseChan() <- struct{}{}
		}
		if err = o.subscribeBitgetDepthMultipleWithZeroCopy(bitgetWsClient, []string{symbol}, callback, isZeroCopy); err != nil {
			log.Error(err)
		}
	}

	go func() {
		for {
			select {
			case err := <-sub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-sub.ResultChan():
				symbol := result.Symbol
				switch result.Action {
				case "snapshot":
					o.OrderBookReadyUpdateIdMap.Delete(symbol)
					o.OrderBookMap.Delete(symbol)
					o.OrderBookLastUpdateIdMap.Delete(symbol)
					o.OrderBookRBTreeMap.Delete(symbol)
					o.initBitgetDepthOrderBook(result)
				case "update":
					_, err := o.checkBitgetDepthIsReady(symbol)
					if err != nil {
						go reSubThis(symbol)
						continue
					}
					if err = o.saveBitgetDepthOrderBook(result); err != nil {
						log.Error(err)
						go reSubThis(symbol)
						continue
					}
					if callback == nil || o.callBackDepthLevel == 0 {
						continue
					}
					if isZeroCopy {
						err = o.ViewDepth(symbol, int(o.callBackDepthLevel), o.callBackDepthTimeoutMilli, func(d *Depth) error {
							d.UId = result.Seq
							d.PreUId = result.PSeq
							callback(d, nil)
							return nil
						})
						if err != nil {
							callback(nil, err)
							continue
						}
					} else {
						depth, err := o.GetDepth(symbol, int(o.callBackDepthLevel), o.callBackDepthTimeoutMilli)
						if err != nil {
							callback(nil, err)
							continue
						}
						depth.UId = result.Seq
						depth.PreUId = result.PSeq
						callback(depth, nil)
					}
				default:
					o.initAndClearBitgetDepthOrderBook(result)
					if err = o.saveBitgetDepthOrderBook(result); err != nil {
						log.Error(err)
						go reSubThis(symbol)
						continue
					}
					if callback == nil || o.callBackDepthLevel == 0 {
						continue
					}
					if isZeroCopy {
						err = o.ViewDepth(symbol, int(o.callBackDepthLevel), o.callBackDepthTimeoutMilli, func(d *Depth) error {
							d.UId = result.Seq
							d.PreUId = result.PSeq
							callback(d, nil)
							return nil
						})
						if err != nil {
							callback(nil, err)
							continue
						}
					} else {
						depth, err := o.GetDepth(symbol, int(o.callBackDepthLevel), o.callBackDepthTimeoutMilli)
						if err != nil {
							callback(nil, err)
							continue
						}
						depth.UId = result.Seq
						depth.PreUId = result.PSeq
						callback(depth, nil)
					}
				}
			case <-sub.CloseChan():
				log.Info("Bitget OrderBook 订阅已关闭: ", sub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := o.WsClientListMap.Load(bitgetWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		o.WsClientListMap.Store(bitgetWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (o *bitgetOrderBookBase) checkBitgetDepthIsReady(symbol string) (int64, error) {
	readyId, isReady := o.OrderBookReadyUpdateIdMap.Load(symbol)
	if !isReady {
		return 0, fmt.Errorf("%s 深度未准备好", symbol)
	}
	return readyId, nil
}

func (o *bitgetOrderBookBase) initBitgetDepthOrderBook(result mybitgetapi.WsBooks) {
	symbol := result.Symbol
	orderBook, ok := o.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		orderBook = NewOrderBook()
		o.OrderBookRBTreeMap.Store(symbol, orderBook)
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
	o.OrderBookReadyUpdateIdMap.Store(symbol, result.Seq)
	o.OrderBookLastUpdateIdMap.Store(symbol, result.Seq)
}

func (o *bitgetOrderBookBase) initAndClearBitgetDepthOrderBook(result mybitgetapi.WsBooks) {
	symbol := result.Symbol
	orderBook, ok := o.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		orderBook = NewOrderBook()
		o.OrderBookRBTreeMap.Store(symbol, orderBook)
	}
	orderBook.ClearAll()
}

func (o *bitgetOrderBookBase) saveBitgetDepthOrderBook(result mybitgetapi.WsBooks) error {
	symbol := result.Symbol
	lastSeq, ok := o.OrderBookLastUpdateIdMap.Load(symbol)
	if ok {
		if result.Seq > result.PSeq {
			// normal
		} else if result.Seq == result.PSeq {
			return nil
		} else if result.Seq < result.PSeq {
			log.Warnf("%s seq reset %d to %d", symbol, result.PSeq, result.Seq)
		}
		_ = lastSeq
	}
	o.OrderBookLastUpdateIdMap.Store(symbol, result.Seq)
	orderBook, ok := o.OrderBookRBTreeMap.Load(symbol)
	if !ok || orderBook == nil {
		orderBook = NewOrderBook()
		o.OrderBookRBTreeMap.Store(symbol, orderBook)
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
	now := time.Now().UnixMilli()
	ts, _ := strconv.ParseInt(result.Ts, 10, 64)
	targetTs := ts + o.parent.parent.GetServerTimeDelta()
	if targetTs > now {
		targetTs = now
	}
	depth := &Depth{
		UId:         result.Seq,
		PreUId:      result.PSeq,
		AccountType: o.AccountType.String(),
		Exchange:    o.Exchange.String(),
		Symbol:      symbol,
		Timestamp:   targetTs,
	}
	o.OrderBookMap.Store(symbol, depth)
	return nil
}

func (o *bitgetOrderBookBase) Close() {
	o.BitgetWsClientBase.close()
	o.OrderBookRBTreeMap.Clear()
	o.OrderBookReadyUpdateIdMap.Clear()
	o.OrderBookMap.Clear()
	o.OrderBookLastUpdateIdMap.Clear()
	o.WsClientMap.Clear()
	o.SubMap.Clear()
	o.CallBackMap.Clear()
	o.ReSubMuMap.Clear()
	o.ReSubWsClientMap.Clear()
}

func (b *BitgetOrderBook) Close() {
	b.SpotOrderBook.Close()
	b.UsdtFuturesOrderBook.Close()
	b.CoinFuturesOrderBook.Close()
	b.UsdcFuturesOrderBook.Close()
}
