package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myokxapi"
)

type OkxAggTrade struct {
	parent          *OkxMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	AggTradeMap     *MySyncMap[string, *AggTrade]
	WsClientListMap *MySyncMap[*myokxapi.PublicWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myokxapi.PublicWsStreamClient]
	SubMap          *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsTrades]]
	CallBackMap     *MySyncMap[string, func(aggTrade *AggTrade, err error)]
}

func (om *OkxMarketData) newOkxAggTrade(config OkxAggTradeConfig) *OkxAggTrade {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	o := &OkxAggTrade{
		parent:          om,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        OKX,
		AggTradeMap:     GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myokxapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myokxapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsTrades]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
	return o
}

func (o *OkxAggTrade) GetLastAggTrade(symbol string) (*AggTrade, error) {
	symbolKey := symbol
	aggTrade, ok := o.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s aggTrade not found", symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (o *OkxAggTrade) GetCurrentOrNewWsClient() (*myokxapi.PublicWsStreamClient, error) {
	return o.parent.GetPublicCurrentOrNewWsClient(o.perConnSubNum, o.WsClientListMap)
}

func (o *OkxAggTrade) SubscribeAggTrade(symbol string) error {
	return o.SubscribeAggTradeWithCallBack(symbol, nil)
}

func (o *OkxAggTrade) SubscribeAggTrades(symbols []string) error {
	return o.SubscribeAggTradesWithCallBack(symbols, nil)
}

func (o *OkxAggTrade) SubscribeAggTradeWithCallBack(symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return o.SubscribeAggTradesWithCallBack([]string{symbol}, callback)
}

func (o *OkxAggTrade) SubscribeAggTradesWithCallBack(symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅AggTrade，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
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
			err = o.subscribeOkxAggTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("归集交易流分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = o.subscribeOkxAggTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// 订阅OKX深度
func (o *OkxAggTrade) subscribeOkxAggTradeMultiple(okxWsClient *myokxapi.PublicWsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {

	okxSub, err := okxWsClient.SubscribeTradesMultiple(symbols)
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
				isMarket := true
				if result.Side == "buy" {
					isMarket = false
				}

				if okx_common == nil {
					okx_common = (&okxCommon{}).InitCommon()
				}

				//保存至AggTrade
				aggTrade := &AggTrade{
					AId:         result.TradeId,
					Exchange:    o.Exchange.String(),
					AccountType: okx_common.GetAccountTypeFromSymbol(result.Trades.InstId),
					Symbol:      result.Trades.InstId,
					Timestamp:   stringToInt64(result.Ts) + o.parent.serverTimeDelta,
					Price:       stringToFloat64(result.Px),
					Quantity:    stringToFloat64(result.Sz),
					TradeTime:   stringToInt64(result.Ts) + o.parent.serverTimeDelta,
					IsMarket:    isMarket,
				}
				o.AggTradeMap.Store(result.Trades.InstId, aggTrade)
				if callback != nil {
					callback(aggTrade, nil)
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

func (o *OkxAggTrade) Close() {
	o.WsClientListMap.Range(func(k *myokxapi.PublicWsStreamClient, v *int64) bool {
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
