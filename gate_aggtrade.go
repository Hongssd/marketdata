package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mygateapi"
	"github.com/shopspring/decimal"
)

type GateAggTrade struct {
	parent           *GateMarketData
	SpotAggTrade     *gateAggTradeBase
	FuturesAggTrade  *gateAggTradeBase
	DeliveryAggTrade *gateAggTradeBase
}

type gateAggTradeBase struct {
	parent *GateAggTrade
	GateWsClientBase
	Exchange    Exchange
	AccountType GateAccountType
	AggTradeMap *MySyncMap[string, *AggTrade]                                                                       //symbol->last aggTrade
	WsClientMap *MySyncMap[string, *mygateapi.WsStreamClient]                                                       //symbol->ws client
	SubMap      *MySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsTrade]]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(aggTrade *AggTrade, err error)]                                             //symbol->callback
}

func (b *GateAggTrade) getBaseMapFromAccountType(accountType GateAccountType) (*gateAggTradeBase, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotAggTrade, nil
	case GATE_FUTURES:
		return b.FuturesAggTrade, nil
	case GATE_DELIVERY:
		return b.DeliveryAggTrade, nil
	}
	return nil, ErrorAccountType
}

func (b *GateAggTrade) newGateAggTradeBase(config GateAggTradeConfigBase) *gateAggTradeBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &gateAggTradeBase{
		Exchange: GATE,
		GateWsClientBase: GateWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mygateapi.WsStreamClient, *int64]()),
		},
		AggTradeMap: GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mygateapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsTrade]]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (b *GateAggTrade) GetLastAggTrade(GateAccountType GateAccountType, symbol string) (*AggTrade, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(GateAccountType)
	if err != nil {
		return nil, err
	}
	aggTrade, ok := bmap.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s aggTrade not found", GateAccountType, symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (b *GateAggTrade) GetCurrentOrNewWsClient(accountType GateAccountType) (*mygateapi.WsStreamClient, error) {

	switch accountType {
	case GATE_SPOT:
		return b.SpotAggTrade.GetCurrentOrNewWsClient(accountType)
	case GATE_FUTURES:
		return b.FuturesAggTrade.GetCurrentOrNewWsClient(accountType)
	case GATE_DELIVERY:
		return b.DeliveryAggTrade.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅币安有限档交易流底层执行
func (b *gateAggTradeBase) subscribeGateAggTradeMultiple(gateWsClient *mygateapi.WsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {

	gateSub, err := gateWsClient.SubscribeTradeMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, gateWsClient)
		b.SubMap.Store(symbolKey, gateSub)
		b.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-gateSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-gateSub.ResultChan():
				result := r.Result
				symbolKey := result.Symbol

				price, _ := decimal.NewFromString(result.Price)
				quantity, _ := decimal.NewFromString(result.Amount)
				var first, last int64
				if result.Range != "" {
					splitRange := strings.Split(result.Range, "-")
					if len(splitRange) == 2 {
						first, _ = strconv.ParseInt(splitRange[0], 10, 64)
						last, _ = strconv.ParseInt(splitRange[1], 10, 64)
					}
				}

				isMarket := false
				if result.Side == "sell" {
					//主动卖，买方是做市方
					isMarket = true
				}

				now := time.Now().UnixMilli()
				targetTs := r.TimeMs + b.parent.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				//保存至AggTrade
				aggTrade := &AggTrade{
					AId:         strconv.FormatInt(result.Id, 10),
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   targetTs,
					Price:       price.InexactFloat64(),
					Quantity:    quantity.InexactFloat64(),
					First:       first,
					Last:        last,
					TradeTime:   targetTs,
					IsMarket:    isMarket,
				}
				b.AggTradeMap.Store(symbolKey, aggTrade)
				if callback != nil {
					callback(aggTrade, nil)
				}
			case <-gateSub.CloseChan():
				log.Info("订阅已关闭: ", gateSub.SubKeys)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(gateWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(gateWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安交易流
func (b *gateAggTradeBase) UnSubscribeGateAggTrade(symbol string) error {
	symbolKey := symbol
	gateSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return gateSub.Unsubscribe()
}

// 订阅交易流
func (b *GateAggTrade) SubscribeAggTrade(accountType GateAccountType, symbol string) error {
	return b.SubscribeAggTradeWithCallBack(accountType, symbol, nil)
}

// 批量订阅交易流
func (b *GateAggTrade) SubscribeAggTrades(accountType GateAccountType, symbols []string) error {
	return b.SubscribeAggTradesWithCallBack(accountType, symbols, nil)
}

// 订阅交易流并带上回调
func (b *GateAggTrade) SubscribeAggTradeWithCallBack(accountType GateAccountType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return b.SubscribeAggTradesWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (b *GateAggTrade) SubscribeAggTradesWithCallBack(accountType GateAccountType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentGateAggTradeBase *gateAggTradeBase

	switch accountType {
	case GATE_SPOT:
		currentGateAggTradeBase = b.SpotAggTrade
	case GATE_FUTURES:
		currentGateAggTradeBase = b.FuturesAggTrade
	case GATE_DELIVERY:
		currentGateAggTradeBase = b.DeliveryAggTrade
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentGateAggTradeBase.perSubMaxLen
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
			err = currentGateAggTradeBase.subscribeGateAggTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentGateAggTradeBase.WsClientListMap.Load(client)
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
		err = currentGateAggTradeBase.subscribeGateAggTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *gateAggTradeBase) Close() {
	b.GateWsClientBase.close()

	b.AggTradeMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *GateAggTrade) Close() {
	b.SpotAggTrade.Close()
	b.FuturesAggTrade.Close()
	b.DeliveryAggTrade.Close()
}
