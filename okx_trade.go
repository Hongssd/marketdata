package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myokxapi"
)

type OkxTrade struct {
	parent          *OkxMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	TradeMap        *MySyncMap[string, *Trade]
	WsClientListMap *MySyncMap[*myokxapi.BusinessWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myokxapi.BusinessWsStreamClient]
	SubMap          *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsAllTrades]]
	CallBackMap     *MySyncMap[string, func(trade *Trade, err error)]
}

func (om *OkxMarketData) newOkxTrade(config OkxTradeConfig) *OkxTrade {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	o := &OkxTrade{
		parent:          om,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        OKX,
		TradeMap:        GetPointer(NewMySyncMap[string, *Trade]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myokxapi.BusinessWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myokxapi.BusinessWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsAllTrades]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(trade *Trade, err error)]()),
	}
	return o
}

func (o *OkxTrade) GetLastTrade(symbol string) (*Trade, error) {
	symbolKey := symbol
	trade, ok := o.TradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s trade not found", symbol)
		return nil, err
	}
	return trade, nil
}

func (o *OkxTrade) GetCurrentOrNewWsClient() (*myokxapi.BusinessWsStreamClient, error) {
	return o.parent.GetBusinessCurrentOrNewWsClient(o.perConnSubNum, o.WsClientListMap)
}

func (o *OkxTrade) SubscribeTrade(symbol string) error {
	return o.SubscribeTradeWithCallBack(symbol, nil)
}

func (o *OkxTrade) SubscribeTrades(symbols []string) error {
	return o.SubscribeTradesWithCallBack(symbols, nil)
}

func (o *OkxTrade) SubscribeTradeWithCallBack(symbol string, callback func(trade *Trade, err error)) error {
	return o.SubscribeTradesWithCallBack([]string{symbol}, callback)
}

func (o *OkxTrade) SubscribeTradesWithCallBack(symbols []string, callback func(trade *Trade, err error)) error {
	log.Infof("开始订阅逐笔交易流，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
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
			err = o.subscribeOkxTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("逐笔交易流分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = o.subscribeOkxTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("逐笔交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// 订阅OKX深度
func (o *OkxTrade) subscribeOkxTradeMultiple(okxWsClient *myokxapi.BusinessWsStreamClient, symbols []string, callback func(trade *Trade, err error)) error {

	okxSub, err := okxWsClient.SubscribeAllTradesMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		symbolKey := symbol
		o.WsClientMap.Store(symbolKey, okxWsClient)
		o.SubMap.Store(symbolKey, okxSub)
		o.CallBackMap.Store(symbolKey, callback)
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
				for _, t := range result.AllTrades {
					if okx_common == nil {
						okx_common = (&okxCommon{}).InitCommon()
					}

					now := time.Now().UnixMilli()
					targetTs := stringToInt64(t.Ts) + o.parent.serverTimeDelta
					if targetTs > now {
						targetTs = now
					}

					//保存至Trade
					trade := &Trade{
						AId:         t.TradeId,
						Exchange:    o.Exchange.String(),
						AccountType: okx_common.GetAccountTypeFromSymbol(t.InstId),
						Symbol:      t.InstId,
						Timestamp:   targetTs,
						Price:       stringToFloat64(t.Px),
						Quantity:    stringToFloat64(t.Sz),
						TradeTime:   targetTs,
						IsBuyer:     t.Side == "buy",
						IsMarket:    false,
					}
					o.TradeMap.Store(t.InstId, trade)
					if callback != nil {
						callback(trade, nil)
					}
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

func (o *OkxTrade) Close() {
	o.WsClientListMap.Range(func(k *myokxapi.BusinessWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	o.WsClientListMap.Clear()
	o.WsClientMap.Clear()
	o.SubMap.Clear()
	o.CallBackMap.Clear()
}
