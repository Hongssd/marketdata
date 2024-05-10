package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mybinanceapi"
	"strconv"
	"sync/atomic"
	"time"
)

type BinanceAggTrade struct {
	parent         *BinanceMarketData
	SpotAggTrade   *binanceAggTradeBase
	FutureAggTrade *binanceAggTradeBase
	SwapAggTrade   *binanceAggTradeBase
}

type binanceAggTradeBase struct {
	parent *BinanceAggTrade
	BinanceWsClientBase
	Exchange    Exchange
	AccountType BinanceAccountType
	AggTradeMap *MySyncMap[string, *AggTrade]                                           //symbol->last aggTrade
	WsClientMap *MySyncMap[string, *mybinanceapi.WsStreamClient]                        //symbol->ws client
	SubMap      *MySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsAggTrade]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(aggTrade *AggTrade, err error)]                 //symbol->callback
}

func (b *BinanceAggTrade) getBaseMapFromAccountType(accountType BinanceAccountType) (*binanceAggTradeBase, error) {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotAggTrade, nil
	case BINANCE_FUTURE:
		return b.FutureAggTrade, nil
	case BINANCE_SWAP:
		return b.SwapAggTrade, nil
	}
	return nil, ErrorAccountType
}

func (b *BinanceAggTrade) newBinanceAggTradeBase(config BinanceAggTradeConfigBase) *binanceAggTradeBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &binanceAggTradeBase{
		Exchange: BINANCE,
		BinanceWsClientBase: BinanceWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
		},
		AggTradeMap: GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsAggTrade]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (b *BinanceAggTrade) GetLastAggTrade(BinanceAccountType BinanceAccountType, symbol string) (*AggTrade, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(BinanceAccountType)
	if err != nil {
		return nil, err
	}
	aggTrade, ok := bmap.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s aggTrade not found", BinanceAccountType, symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (b *BinanceAggTrade) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {

	switch accountType {
	case BINANCE_SPOT:
		return b.SpotAggTrade.GetCurrentOrNewWsClient(accountType)
	case BINANCE_FUTURE:
		return b.FutureAggTrade.GetCurrentOrNewWsClient(accountType)
	case BINANCE_SWAP:
		return b.SwapAggTrade.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅币安有限档交易流底层执行
func (b *binanceAggTradeBase) subscribeBinanceAggTradeMultiple(binanceWsClient *mybinanceapi.WsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {

	binanceSub, err := binanceWsClient.SubscribeAggTradeMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, binanceWsClient)
		b.SubMap.Store(symbolKey, binanceSub)
		b.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-binanceSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-binanceSub.ResultChan():
				symbolKey := result.Symbol

				//保存至AggTrade
				aggTrade := &AggTrade{
					AId:         strconv.FormatInt(result.AId, 10),
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType),
					Price:       result.Price,
					Quantity:    result.Quantity,
					First:       result.First,
					Last:        result.Last,
					TradeTime:   result.TradeTime,
					IsMarket:    result.IsMarket,
				}
				b.AggTradeMap.Store(symbolKey, aggTrade)
				if callback != nil {
					callback(aggTrade, nil)
				}
			case <-binanceSub.CloseChan():
				log.Info("订阅已关闭: ", binanceSub.Params)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(binanceWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(binanceWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安交易流
func (b *binanceAggTradeBase) UnSubscribeBinanceAggTrade(symbol string) error {
	symbolKey := symbol
	binanceSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return binanceSub.Unsubscribe()
}

// 订阅交易流
func (b *BinanceAggTrade) SubscribeAggTrade(accountType BinanceAccountType, symbol string) error {
	return b.SubscribeAggTradeWithCallBack(accountType, symbol, nil)
}

// 批量订阅交易流
func (b *BinanceAggTrade) SubscribeAggTrades(accountType BinanceAccountType, symbols []string) error {
	return b.SubscribeAggTradesWithCallBack(accountType, symbols, nil)
}

// 订阅交易流并带上回调
func (b *BinanceAggTrade) SubscribeAggTradeWithCallBack(accountType BinanceAccountType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return b.SubscribeAggTradesWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (b *BinanceAggTrade) SubscribeAggTradesWithCallBack(accountType BinanceAccountType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBinanceAggTradeBase *binanceAggTradeBase

	switch accountType {
	case BINANCE_SPOT:
		currentBinanceAggTradeBase = b.SpotAggTrade
	case BINANCE_FUTURE:
		currentBinanceAggTradeBase = b.FutureAggTrade
	case BINANCE_SWAP:
		currentBinanceAggTradeBase = b.SwapAggTrade
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentBinanceAggTradeBase.perSubMaxLen
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
			err = currentBinanceAggTradeBase.subscribeBinanceAggTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBinanceAggTradeBase.WsClientListMap.Load(client)
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
		err = currentBinanceAggTradeBase.subscribeBinanceAggTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}
