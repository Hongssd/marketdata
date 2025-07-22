package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybinanceapi"
)

type BinanceTickers struct {
	parent        *BinanceMarketData
	SpotTickers   *binanceTickersBase
	FutureTickers *binanceTickersBase
	SwapTickers   *binanceTickersBase
}

type binanceTickersBase struct {
	parent *BinanceTickers
	BinanceWsClientBase
	Exchange    Exchange
	AccountType BinanceAccountType
	TickersMap  *MySyncMap[string, *Ticker]                                           //symbol->last ticker
	WsClientMap *MySyncMap[string, *mybinanceapi.WsStreamClient]                      //symbol->ws client
	SubMap      *MySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsTicker]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(ticker *Ticker, err error)]                   //symbol->callback
	// 全部ticker订阅相关
	AllTickersSubMap      *MySyncMap[string, *mybinanceapi.Subscription[[]*mybinanceapi.WsTicker]] //allTickers->subscribe
	AllTickersCallBackMap *MySyncMap[string, func(tickers []*Ticker, err error)]                   //allTickers->callback
}

func (b *BinanceTickers) getBaseMapFromAccountType(accountType BinanceAccountType) (*binanceTickersBase, error) {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotTickers, nil
	case BINANCE_FUTURE:
		return b.FutureTickers, nil
	case BINANCE_SWAP:
		return b.SwapTickers, nil
	}
	return nil, ErrorAccountType
}

func (b *BinanceTickers) newBinanceTickersBase(config BinanceTickersConfigBase) *binanceTickersBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &binanceTickersBase{
		Exchange: BINANCE,
		BinanceWsClientBase: BinanceWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
		},
		TickersMap:            GetPointer(NewMySyncMap[string, *Ticker]()),
		WsClientMap:           GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:                GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsTicker]]()),
		CallBackMap:           GetPointer(NewMySyncMap[string, func(ticker *Ticker, err error)]()),
		AllTickersSubMap:      GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[[]*mybinanceapi.WsTicker]]()),
		AllTickersCallBackMap: GetPointer(NewMySyncMap[string, func(tickers []*Ticker, err error)]()),
	}
}

// 封装好的获取ticker方法
func (b *BinanceTickers) GetLastTicker(BinanceAccountType BinanceAccountType, symbol string) (*Ticker, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(BinanceAccountType)
	if err != nil {
		return nil, err
	}
	ticker, ok := bmap.TickersMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s ticker not found", BinanceAccountType, symbol)
		return nil, err
	}
	return ticker, nil
}

func (b *BinanceTickers) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {

	switch accountType {
	case BINANCE_SPOT:
		return b.SpotTickers.GetCurrentOrNewWsClient(accountType)
	case BINANCE_FUTURE:
		return b.FutureTickers.GetCurrentOrNewWsClient(accountType)
	case BINANCE_SWAP:
		return b.SwapTickers.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅币安单个ticker底层执行
func (b *binanceTickersBase) subscribeBinanceTickerSingle(binanceWsClient *mybinanceapi.WsStreamClient, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.subscribeBinanceTickerMultiple(binanceWsClient, []string{symbol}, callback)
}

// 订阅币安多个ticker底层执行
func (b *binanceTickersBase) subscribeBinanceTickerMultiple(binanceWsClient *mybinanceapi.WsStreamClient, symbols []string, callback func(ticker *Ticker, err error)) error {

	binanceSub, err := binanceWsClient.SubscribeTickerMultiple(symbols)
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
				//保存至Ticker
				ticker := &Ticker{
					Timestamp:       targetTs,
					Exchange:        b.Exchange.String(),
					AccountType:     b.AccountType.String(),
					Symbol:          result.Symbol,
					LastPrice:       stringToFloat64(result.LastPrice),
					Price24hPercent: stringToFloat64(result.PriceChangePercent),
					PrevPrice24h:    stringToFloat64(result.PrevClosePrice),
					HighPrice24h:    stringToFloat64(result.HighPrice),
					LowPrice24h:     stringToFloat64(result.LowPrice),
					Volume24h:       stringToFloat64(result.Volume),
					Turnover24h:     stringToFloat64(result.QuoteVolume),
				}
				b.TickersMap.Store(symbolKey, ticker)
				if callback != nil {
					callback(ticker, nil)
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

// 订阅币安全部ticker底层执行
func (b *binanceTickersBase) subscribeBinanceTickerAll(binanceWsClient *mybinanceapi.WsStreamClient, callback func(tickers []*Ticker, err error)) error {

	binanceSub, err := binanceWsClient.SubscribeTickerAll()
	if err != nil {
		log.Error(err)
		return err
	}

	key := "all"
	b.AllTickersSubMap.Store(key, binanceSub)
	b.AllTickersCallBackMap.Store(key, callback)

	go func() {
		for {
			select {
			case err := <-binanceSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case results := <-binanceSub.ResultChan():
				now := time.Now().UnixMilli()
				var tickers []*Ticker

				for _, result := range results {
					targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType)
					if targetTs > now {
						targetTs = now
					}

					//保存至Ticker
					ticker := &Ticker{
						Timestamp:       targetTs,
						Exchange:        b.Exchange.String(),
						AccountType:     b.AccountType.String(),
						Symbol:          result.Symbol,
						LastPrice:       stringToFloat64(result.LastPrice),
						Price24hPercent: stringToFloat64(result.PriceChangePercent),
						PrevPrice24h:    stringToFloat64(result.PrevClosePrice),
						HighPrice24h:    stringToFloat64(result.HighPrice),
						LowPrice24h:     stringToFloat64(result.LowPrice),
						Volume24h:       stringToFloat64(result.Volume),
						Turnover24h:     stringToFloat64(result.QuoteVolume),
					}

					b.TickersMap.Store(result.Symbol, ticker)
					tickers = append(tickers, ticker)
				}

				if callback != nil {
					callback(tickers, nil)
				}
			case <-binanceSub.CloseChan():
				log.Info("订阅已关闭: ", binanceSub.Params)
				return
			}
		}
	}()

	currentCount := int64(1) // 全部ticker订阅只算1个订阅
	count, ok := b.WsClientListMap.Load(binanceWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(binanceWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安ticker
func (b *binanceTickersBase) UnSubscribeBinanceTicker(symbol string) error {
	symbolKey := symbol
	binanceSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return binanceSub.Unsubscribe()
}

// 取消订阅币安全部ticker
func (b *binanceTickersBase) UnSubscribeBinanceTickerAll() error {
	key := "all"
	binanceSub, ok := b.AllTickersSubMap.Load(key)
	if !ok {
		return nil
	}
	return binanceSub.Unsubscribe()
}

// 订阅ticker
func (b *BinanceTickers) SubscribeTicker(accountType BinanceAccountType, symbol string) error {
	return b.SubscribeTickerWithCallBack(accountType, symbol, nil)
}

// 批量订阅ticker
func (b *BinanceTickers) SubscribeTickers(accountType BinanceAccountType, symbols []string) error {
	return b.SubscribeTickersWithCallBack(accountType, symbols, nil)
}

// 订阅全部ticker
func (b *BinanceTickers) SubscribeTickerAll(accountType BinanceAccountType) error {
	return b.SubscribeTickerAllWithCallBack(accountType, nil)
}

// 订阅ticker并带上回调
func (b *BinanceTickers) SubscribeTickerWithCallBack(accountType BinanceAccountType, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.SubscribeTickersWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅ticker并带上回调
func (b *BinanceTickers) SubscribeTickersWithCallBack(accountType BinanceAccountType, symbols []string, callback func(ticker *Ticker, err error)) error {
	log.Infof("开始订阅Ticker%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBinanceTickersBase *binanceTickersBase

	switch accountType {
	case BINANCE_SPOT:
		currentBinanceTickersBase = b.SpotTickers
	case BINANCE_FUTURE:
		currentBinanceTickersBase = b.FutureTickers
	case BINANCE_SWAP:
		currentBinanceTickersBase = b.SwapTickers
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentBinanceTickersBase.perSubMaxLen
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
			err = currentBinanceTickersBase.subscribeBinanceTickerMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBinanceTickersBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Ticker%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentBinanceTickersBase.subscribeBinanceTickerMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("Ticker订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// 订阅全部ticker并带上回调
func (b *BinanceTickers) SubscribeTickerAllWithCallBack(accountType BinanceAccountType, callback func(tickers []*Ticker, err error)) error {
	log.Infof("开始订阅全部Ticker%s", accountType)

	var currentBinanceTickersBase *binanceTickersBase

	switch accountType {
	case BINANCE_SPOT:
		currentBinanceTickersBase = b.SpotTickers
	case BINANCE_FUTURE:
		currentBinanceTickersBase = b.FutureTickers
	case BINANCE_SWAP:
		currentBinanceTickersBase = b.SwapTickers
	default:
		return ErrorAccountType
	}

	client, err := b.GetCurrentOrNewWsClient(accountType)
	if err != nil {
		return err
	}
	err = currentBinanceTickersBase.subscribeBinanceTickerAll(client, callback)
	if err != nil {
		return err
	}

	log.Infof("全部Ticker%s订阅结束", accountType)

	return nil
}

func (b *binanceTickersBase) Close() {
	b.BinanceWsClientBase.close()

	b.TickersMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()
	b.AllTickersSubMap.Clear()
	b.AllTickersCallBackMap.Clear()

}

func (b *BinanceTickers) Close() {
	b.SpotTickers.Close()
	b.FutureTickers.Close()
	b.SwapTickers.Close()
}
