package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybinanceapi"
)

type BinanceTrade struct {
	parent      *BinanceMarketData
	SpotTrade   *binanceTradeBase
	FutureTrade *binanceTradeBase
	SwapTrade   *binanceTradeBase
}

type binanceTradeBase struct {
	parent *BinanceTrade
	BinanceWsClientBase
	Exchange    Exchange
	AccountType BinanceAccountType
	TradeMap    *MySyncMap[string, *Trade]                                           //symbol->last trade
	WsClientMap *MySyncMap[string, *mybinanceapi.WsStreamClient]                     //symbol->ws client
	SubMap      *MySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsTrade]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(trade *Trade, err error)]                    //symbol->callback
}

func (b *BinanceTrade) getBaseMapFromAccountType(accountType BinanceAccountType) (*binanceTradeBase, error) {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotTrade, nil
	case BINANCE_FUTURE:
		return b.FutureTrade, nil
	case BINANCE_SWAP:
		return b.SwapTrade, nil
	}
	return nil, ErrorAccountType
}

func (b *BinanceTrade) newBinanceTradeBase(config BinanceTradeConfigBase) *binanceTradeBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &binanceTradeBase{
		Exchange: BINANCE,
		BinanceWsClientBase: BinanceWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
		},
		TradeMap:    GetPointer(NewMySyncMap[string, *Trade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsTrade]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(trade *Trade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (b *BinanceTrade) GetLastTrade(BinanceAccountType BinanceAccountType, symbol string) (*Trade, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(BinanceAccountType)
	if err != nil {
		return nil, err
	}
	trade, ok := bmap.TradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s trade not found", BinanceAccountType, symbol)
		return nil, err
	}
	return trade, nil
}

func (b *BinanceTrade) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {

	switch accountType {
	case BINANCE_SPOT:
		return b.SpotTrade.GetCurrentOrNewWsClient(accountType)
	case BINANCE_FUTURE:
		return b.FutureTrade.GetCurrentOrNewWsClient(accountType)
	case BINANCE_SWAP:
		return b.SwapTrade.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅币安有限档交易流底层执行
func (b *binanceTradeBase) subscribeBinanceTradeMultiple(binanceWsClient *mybinanceapi.WsStreamClient, symbols []string, callback func(trade *Trade, err error)) error {

	binanceSub, err := binanceWsClient.SubscribeTradeMultiple(symbols)
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
				now := time.Now().UnixMilli()
				targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType)
				if targetTs > now {
					targetTs = now
				}
				//保存至AggTrade
				trade := &Trade{
					AId:         strconv.FormatInt(result.AId, 10),
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   targetTs,
					Price:       result.Price,
					Quantity:    result.Quantity,
					TradeTime:   targetTs,
					IsMarket:    result.IsMarket,
				}
				b.TradeMap.Store(symbolKey, trade)
				if callback != nil {
					callback(trade, nil)
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
func (b *binanceTradeBase) UnSubscribeBinanceTrade(symbol string) error {
	symbolKey := symbol
	binanceSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return binanceSub.Unsubscribe()
}

// 订阅交易流
func (b *BinanceTrade) SubscribeTrade(accountType BinanceAccountType, symbol string) error {
	return b.SubscribeTradeWithCallBack(accountType, symbol, nil)
}

// 批量订阅交易流
func (b *BinanceTrade) SubscribeTrades(accountType BinanceAccountType, symbols []string) error {
	return b.SubscribeTradesWithCallBack(accountType, symbols, nil)
}

// 订阅交易流并带上回调
func (b *BinanceTrade) SubscribeTradeWithCallBack(accountType BinanceAccountType, symbol string, callback func(trade *Trade, err error)) error {
	return b.SubscribeTradesWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (b *BinanceTrade) SubscribeTradesWithCallBack(accountType BinanceAccountType, symbols []string, callback func(trade *Trade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBinanceTradeBase *binanceTradeBase

	switch accountType {
	case BINANCE_SPOT:
		currentBinanceTradeBase = b.SpotTrade
	case BINANCE_FUTURE:
		currentBinanceTradeBase = b.FutureTrade
	case BINANCE_SWAP:
		currentBinanceTradeBase = b.SwapTrade
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentBinanceTradeBase.perSubMaxLen
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
			err = currentBinanceTradeBase.subscribeBinanceTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBinanceTradeBase.WsClientListMap.Load(client)
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
		err = currentBinanceTradeBase.subscribeBinanceTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("逐笔交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *binanceTradeBase) Close() {
	b.BinanceWsClientBase.close()

	b.TradeMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *BinanceTrade) Close() {
	b.SpotTrade.Close()
	b.FutureTrade.Close()
	b.SwapTrade.Close()
}
