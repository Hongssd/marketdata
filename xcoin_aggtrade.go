package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myxcoinapi"
)

type XcoinAggTrade struct {
	parent          *XcoinMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	AggTradeMap     *MySyncMap[string, *AggTrade]
	WsClientListMap *MySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myxcoinapi.PublicWsStreamClient]
	SubMap          *MySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsTrade]]
	CallBackMap     *MySyncMap[string, func(aggTrade *AggTrade, err error)]
}

func (x *XcoinAggTrade) getBaseMapFromBusinessType(businessType XcoinBusinessType) (*myxcoinapi.PublicWsStreamClient, error) {
	return x.parent.GetPublicCurrentOrNewWsClient(x.perConnSubNum, x.WsClientListMap)
}

func (xm *XcoinMarketData) newXcoinAggTrade(config XcoinAggTradeConfig) *XcoinAggTrade {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &XcoinAggTrade{
		parent:          xm,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        XCOIN,
		AggTradeMap:     GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myxcoinapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsTrade]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (x *XcoinAggTrade) GetLastAggTrade(businessType XcoinBusinessType, symbol string) (*AggTrade, error) {
	symbolKey := symbol

	aggTrade, ok := x.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s aggTrade not found", symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (x *XcoinAggTrade) GetCurrentOrNewWsClient() (*myxcoinapi.PublicWsStreamClient, error) {
	return x.parent.GetPublicCurrentOrNewWsClient(x.perConnSubNum, x.WsClientListMap)
}

// 订阅Xcoin有限档交易流底层执行
func (x *XcoinAggTrade) subscribeXcoinAggTradeMultiple(client *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	xcoinSub, err := client.SubscribeTradeMulti(businessType.String(), symbols...)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		x.WsClientMap.Store(symbolKey, client)
		x.SubMap.Store(symbolKey, xcoinSub)
		x.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-xcoinSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-xcoinSub.ResultChan():
				symbolKey := r.WsSubscribeArg.Symbol

				now := time.Now().UnixMilli()
				targetTs := r.Ts + x.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				aggTrade := &AggTrade{
					AId:         r.Id,
					Exchange:    x.Exchange.String(),
					AccountType: r.BusinessType,
					Symbol:      symbolKey,
					Price:       stringToFloat64(r.Price),
					Quantity:    stringToFloat64(r.Qty),
					Timestamp:   targetTs,
					TradeTime:   targetTs,
					IsMarket:    r.Side == "buy",
					Last:        stringToInt64(r.Id),
					First:       stringToInt64(r.Id),
				}
				x.AggTradeMap.Store(symbolKey, aggTrade)
				if callback != nil {
					callback(aggTrade, nil)
				}

			case <-xcoinSub.CloseChan():
				log.Info("订阅已关闭: ", xcoinSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := x.WsClientListMap.Load(client)
	if !ok {
		initCount := int64(0)
		count = &initCount
		x.WsClientListMap.Store(client, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅交易流
func (x *XcoinAggTrade) UnSubscribeXcoinAggTrade(businessType XcoinBusinessType, symbol string) error {
	symbolKey := symbol
	_, ok := x.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	client, ok := x.WsClientMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return client.UnsubscribeTradeMulti(businessType.String(), symbol)
}

// 订阅交易流
func (x *XcoinAggTrade) SubscribeAggTrade(businessType XcoinBusinessType, symbol string) error {
	return x.SubscribeAggTradeWithCallBack(businessType, symbol, nil)
}

// 批量订阅交易流
func (x *XcoinAggTrade) SubscribeAggTrades(businessType XcoinBusinessType, symbols []string) error {
	return x.SubscribeAggTradesWithCallBack(businessType, symbols, nil)
}

// 订阅交易流并带上回调
func (x *XcoinAggTrade) SubscribeAggTradeWithCallBack(businessType XcoinBusinessType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return x.SubscribeAggTradesWithCallBack(businessType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (x *XcoinAggTrade) SubscribeAggTradesWithCallBack(businessType XcoinBusinessType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", businessType, len(symbols), len(symbols))

	LEN := x.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := x.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = x.subscribeXcoinAggTradeMultiple(client, businessType, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := x.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("归集交易流%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", businessType, tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := x.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = x.subscribeXcoinAggTradeMultiple(client, businessType, symbols, callback)
		if err != nil {
			return err
		}
	}
	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))
	return nil
}

func (x *XcoinAggTrade) Close() {
	x.WsClientListMap.Range(func(k *myxcoinapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})
	x.WsClientListMap.Clear()

	x.AggTradeMap.Clear()
	x.WsClientMap.Clear()
	x.SubMap.Clear()
	x.CallBackMap.Clear()
}
