package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mybybitapi"
	"sync/atomic"
	"time"
)

type BybitAggTrade struct {
	parent          *BybitMarketData
	SpotAggTrade    *bybitAggTradeBase
	LinearAggTrade  *bybitAggTradeBase
	InverseAggTrade *bybitAggTradeBase
}

type bybitAggTradeBase struct {
	parent *BybitAggTrade
	BybitWsClientBase
	Exchange    Exchange
	AccountType BybitAccountType
	AggTradeMap *MySyncMap[string, *AggTrade]                                    //symbol->last aggTrade
	WsClientMap *MySyncMap[string, *mybybitapi.PublicWsStreamClient]             //symbol->ws client
	SubMap      *MySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsTrade]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(aggTrade *AggTrade, err error)]          //symbol->callback
}

func (b *BybitAggTrade) getBaseMapFromAccountType(accountType BybitAccountType) (*bybitAggTradeBase, error) {
	switch accountType {
	case BYBIT_SPOT:
		return b.SpotAggTrade, nil
	case BYBIT_LINEAR:
		return b.LinearAggTrade, nil
	case BYBIT_INVERSE:
		return b.InverseAggTrade, nil
	}
	return nil, ErrorAccountType
}

func (b *BybitAggTrade) newBybitAggTradeBase(config BybitAggTradeConfigBase) *bybitAggTradeBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &bybitAggTradeBase{
		Exchange: BYBIT,
		BybitWsClientBase: BybitWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybybitapi.PublicWsStreamClient, *int64]()),
		},
		AggTradeMap: GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybybitapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsTrade]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (b *BybitAggTrade) GetLastAggTrade(BybitAccountType BybitAccountType, symbol string) (*AggTrade, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(BybitAccountType)
	if err != nil {
		return nil, err
	}
	aggTrade, ok := bmap.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s aggTrade not found", BybitAccountType, symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (b *BybitAggTrade) GetCurrentOrNewWsClient(accountType BybitAccountType) (*mybybitapi.PublicWsStreamClient, error) {

	switch accountType {
	case BYBIT_SPOT:
		return b.SpotAggTrade.GetCurrentOrNewWsClient(accountType)
	case BYBIT_LINEAR:
		return b.LinearAggTrade.GetCurrentOrNewWsClient(accountType)
	case BYBIT_INVERSE:
		return b.InverseAggTrade.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅Bybit交易流底层执行
func (b *bybitAggTradeBase) subscribeBybitAggTradeMultiple(bybitWsClient *mybybitapi.PublicWsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {

	bybitSub, err := bybitWsClient.SubscribeTradeMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, bybitWsClient)
		b.SubMap.Store(symbolKey, bybitSub)
		b.CallBackMap.Store(symbolKey, callback)
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
				if len(result.Data) == 0 {
					continue
				}
				for _, data := range result.Data {
					symbolKey := data.Symbol

					isMarket := false
					if data.Side == "Buy" {
						//买方是吃单方
						isMarket = false
					} else {
						//买方是做市方
						isMarket = true
					}
					now := time.Now().UnixMilli()
					targetTs := data.Timestamp + b.parent.parent.GetServerTimeDelta()
					if targetTs > now {
						targetTs = now
					}
					//保存至AggTrade
					aggTrade := &AggTrade{
						AId:         data.TradeId,
						Exchange:    b.Exchange.String(),
						AccountType: b.AccountType.String(),
						Symbol:      data.Symbol,
						Timestamp:   targetTs,
						Price:       stringToFloat64(data.Price),
						Quantity:    stringToFloat64(data.Volume),
						First:       0,
						Last:        0,
						TradeTime:   targetTs,
						IsMarket:    isMarket,
					}
					b.AggTradeMap.Store(symbolKey, aggTrade)
					if callback != nil {
						callback(aggTrade, nil)
					}
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

// 取消订阅Bybit交易流
func (b *bybitAggTradeBase) UnSubscribeBybitAggTrade(symbol string) error {
	symbolKey := symbol
	bybitSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return bybitSub.Unsubscribe()
}

// 订阅交易流
func (b *BybitAggTrade) SubscribeAggTrade(accountType BybitAccountType, symbol string) error {
	return b.SubscribeAggTradeWithCallBack(accountType, symbol, nil)
}

// 批量订阅交易流
func (b *BybitAggTrade) SubscribeAggTrades(accountType BybitAccountType, symbols []string) error {
	return b.SubscribeAggTradesWithCallBack(accountType, symbols, nil)
}

// 订阅交易流并带上回调
func (b *BybitAggTrade) SubscribeAggTradeWithCallBack(accountType BybitAccountType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return b.SubscribeAggTradesWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (b *BybitAggTrade) SubscribeAggTradesWithCallBack(accountType BybitAccountType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBybitAggTradeBase *bybitAggTradeBase

	switch accountType {
	case BYBIT_SPOT:
		currentBybitAggTradeBase = b.SpotAggTrade
	case BYBIT_LINEAR:
		currentBybitAggTradeBase = b.LinearAggTrade
	case BYBIT_INVERSE:
		currentBybitAggTradeBase = b.InverseAggTrade
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentBybitAggTradeBase.perSubMaxLen
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
			err = currentBybitAggTradeBase.subscribeBybitAggTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBybitAggTradeBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("归集交易流%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentBybitAggTradeBase.subscribeBybitAggTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *bybitAggTradeBase) Close() {
	b.BybitWsClientBase.close()

	b.AggTradeMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *BybitAggTrade) Close() {
	b.SpotAggTrade.Close()
	b.LinearAggTrade.Close()
	b.InverseAggTrade.Close()
}
