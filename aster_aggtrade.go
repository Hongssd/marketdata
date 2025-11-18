package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myasterapi"
)

type AsterAggTrade struct {
	parent         *AsterMarketData
	SpotAggTrade   *asterAggTradeBase
	FutureAggTrade *asterAggTradeBase
}

type asterAggTradeBase struct {
	parent *AsterAggTrade
	AsterWsClientBase
	Exchange    Exchange
	AccountType AsterAccountType
	AggTradeMap *MySyncMap[string, *AggTrade]                                       //symbol->last aggTrade
	WsClientMap *MySyncMap[string, *myasterapi.WsStreamClient]                      //symbol->ws client
	SubMap      *MySyncMap[string, *myasterapi.Subscription[myasterapi.WsAggTrade]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(aggTrade *AggTrade, err error)]             //symbol->callback
}

func (b *AsterAggTrade) getBaseMapFromAccountType(accountType AsterAccountType) (*asterAggTradeBase, error) {
	switch accountType {
	case ASTER_SPOT:
		return b.SpotAggTrade, nil
	case ASTER_FUTURE:
		return b.FutureAggTrade, nil
	}
	return nil, ErrorAccountType
}

func (b *AsterAggTrade) newAsterAggTradeBase(config AsterAggTradeConfigBase) *asterAggTradeBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &asterAggTradeBase{
		Exchange: ASTER,
		AsterWsClientBase: AsterWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*myasterapi.WsStreamClient, *int64]()),
		},
		AggTradeMap: GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *myasterapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *myasterapi.Subscription[myasterapi.WsAggTrade]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (b *AsterAggTrade) GetLastAggTrade(AsterAccountType AsterAccountType, symbol string) (*AggTrade, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(AsterAccountType)
	if err != nil {
		return nil, err
	}
	aggTrade, ok := bmap.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s aggTrade not found", AsterAccountType, symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (b *AsterAggTrade) GetCurrentOrNewWsClient(accountType AsterAccountType) (*myasterapi.WsStreamClient, error) {

	switch accountType {
	case ASTER_SPOT:
		return b.SpotAggTrade.GetCurrentOrNewWsClient(accountType)
	case ASTER_FUTURE:
		return b.FutureAggTrade.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅币安有限档交易流底层执行
func (b *asterAggTradeBase) subscribeAsterAggTradeMultiple(asterWsClient *myasterapi.WsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {

	asterSub, err := asterWsClient.SubscribeAggTradeMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, asterWsClient)
		b.SubMap.Store(symbolKey, asterSub)
		b.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-asterSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-asterSub.ResultChan():
				symbolKey := result.Symbol
				now := time.Now().UnixMilli()
				targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType)
				if targetTs > now {
					targetTs = now
				}
				//保存至AggTrade
				aggTrade := &AggTrade{
					AId:         strconv.FormatInt(result.AId, 10),
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   targetTs,
					Price:       result.Price,
					Quantity:    result.Quantity,
					First:       result.First,
					Last:        result.Last,
					TradeTime:   targetTs,
					IsMarket:    result.IsMarket,
				}
				b.AggTradeMap.Store(symbolKey, aggTrade)
				if callback != nil {
					callback(aggTrade, nil)
				}
			case <-asterSub.CloseChan():
				log.Info("订阅已关闭: ", asterSub.Params)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(asterWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(asterWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安交易流
func (b *asterAggTradeBase) UnSubscribeAsterAggTrade(symbol string) error {
	symbolKey := symbol
	asterSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return asterSub.Unsubscribe()
}

// 订阅交易流
func (b *AsterAggTrade) SubscribeAggTrade(accountType AsterAccountType, symbol string) error {
	return b.SubscribeAggTradeWithCallBack(accountType, symbol, nil)
}

// 批量订阅交易流
func (b *AsterAggTrade) SubscribeAggTrades(accountType AsterAccountType, symbols []string) error {
	return b.SubscribeAggTradesWithCallBack(accountType, symbols, nil)
}

// 订阅交易流并带上回调
func (b *AsterAggTrade) SubscribeAggTradeWithCallBack(accountType AsterAccountType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return b.SubscribeAggTradesWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (b *AsterAggTrade) SubscribeAggTradesWithCallBack(accountType AsterAccountType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentAsterAggTradeBase *asterAggTradeBase

	switch accountType {
	case ASTER_SPOT:
		currentAsterAggTradeBase = b.SpotAggTrade
	case ASTER_FUTURE:
		currentAsterAggTradeBase = b.FutureAggTrade
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentAsterAggTradeBase.perSubMaxLen
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
			err = currentAsterAggTradeBase.subscribeAsterAggTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentAsterAggTradeBase.WsClientListMap.Load(client)
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
		err = currentAsterAggTradeBase.subscribeAsterAggTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *asterAggTradeBase) Close() {
	b.AsterWsClientBase.close()

	b.AggTradeMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *AsterAggTrade) Close() {
	b.SpotAggTrade.Close()
	b.FutureAggTrade.Close()
}
