package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myasterapi"
)

type AsterTickers struct {
	parent        *AsterMarketData
	SpotTickers   *asterTickersBase
	FutureTickers *asterTickersBase
}

type asterTickersBase struct {
	parent *AsterTickers
	AsterWsClientBase
	Exchange    Exchange
	AccountType AsterAccountType
	TickersMap  *MySyncMap[string, *Ticker]                                       //symbol->last ticker
	WsClientMap *MySyncMap[string, *myasterapi.WsStreamClient]                    //symbol->ws client
	SubMap      *MySyncMap[string, *myasterapi.Subscription[myasterapi.WsTicker]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(ticker *Ticker, err error)]               //symbol->callback
	// 全部ticker订阅相关
	AllTickersSubMap      *MySyncMap[string, *myasterapi.Subscription[[]*myasterapi.WsTicker]] //allTickers->subscribe
	AllTickersCallBackMap *MySyncMap[string, func(tickers []*Ticker, err error)]               //allTickers->callback
}

func (b *AsterTickers) getBaseMapFromAccountType(accountType AsterAccountType) (*asterTickersBase, error) {
	switch accountType {
	case ASTER_SPOT:
		return b.SpotTickers, nil
	case ASTER_FUTURE:
		return b.FutureTickers, nil
	}
	return nil, ErrorAccountType
}

func (b *AsterTickers) newAsterTickersBase(config AsterTickersConfigBase) *asterTickersBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &asterTickersBase{
		Exchange: ASTER,
		AsterWsClientBase: AsterWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*myasterapi.WsStreamClient, *int64]()),
		},
		TickersMap:            GetPointer(NewMySyncMap[string, *Ticker]()),
		WsClientMap:           GetPointer(NewMySyncMap[string, *myasterapi.WsStreamClient]()),
		SubMap:                GetPointer(NewMySyncMap[string, *myasterapi.Subscription[myasterapi.WsTicker]]()),
		CallBackMap:           GetPointer(NewMySyncMap[string, func(ticker *Ticker, err error)]()),
		AllTickersSubMap:      GetPointer(NewMySyncMap[string, *myasterapi.Subscription[[]*myasterapi.WsTicker]]()),
		AllTickersCallBackMap: GetPointer(NewMySyncMap[string, func(tickers []*Ticker, err error)]()),
	}
}

// 封装好的获取ticker方法
func (b *AsterTickers) GetLastTicker(AsterAccountType AsterAccountType, symbol string) (*Ticker, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(AsterAccountType)
	if err != nil {
		return nil, err
	}
	ticker, ok := bmap.TickersMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s ticker not found", AsterAccountType, symbol)
		return nil, err
	}
	return ticker, nil
}

func (b *AsterTickers) GetCurrentOrNewWsClient(accountType AsterAccountType) (*myasterapi.WsStreamClient, error) {

	switch accountType {
	case ASTER_SPOT:
		return b.SpotTickers.GetCurrentOrNewWsClient(accountType)
	case ASTER_FUTURE:
		return b.FutureTickers.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅币安单个ticker底层执行
func (b *asterTickersBase) subscribeAsterTickerSingle(asterWsClient *myasterapi.WsStreamClient, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.subscribeAsterTickerMultiple(asterWsClient, []string{symbol}, callback)
}

// 订阅币安多个ticker底层执行
func (b *asterTickersBase) subscribeAsterTickerMultiple(asterWsClient *myasterapi.WsStreamClient, symbols []string, callback func(ticker *Ticker, err error)) error {

	asterSub, err := asterWsClient.SubscribeTickerMultiple(symbols)
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

// 订阅币安全部ticker底层执行
func (b *asterTickersBase) subscribeAsterTickerAll(asterWsClient *myasterapi.WsStreamClient, callback func(tickers []*Ticker, err error)) error {

	asterSub, err := asterWsClient.SubscribeTickerAll()
	if err != nil {
		log.Error(err)
		return err
	}

	key := "all"
	b.AllTickersSubMap.Store(key, asterSub)
	b.AllTickersCallBackMap.Store(key, callback)

	go func() {
		for {
			select {
			case err := <-asterSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case results := <-asterSub.ResultChan():
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
			case <-asterSub.CloseChan():
				log.Info("订阅已关闭: ", asterSub.Params)
				return
			}
		}
	}()

	currentCount := int64(1) // 全部ticker订阅只算1个订阅
	count, ok := b.WsClientListMap.Load(asterWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(asterWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安ticker
func (b *asterTickersBase) UnSubscribeAsterTicker(symbol string) error {
	symbolKey := symbol
	asterSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return asterSub.Unsubscribe()
}

// 取消订阅币安全部ticker
func (b *asterTickersBase) UnSubscribeAsterTickerAll() error {
	key := "all"
	asterSub, ok := b.AllTickersSubMap.Load(key)
	if !ok {
		return nil
	}
	return asterSub.Unsubscribe()
}

// 订阅ticker
func (b *AsterTickers) SubscribeTicker(accountType AsterAccountType, symbol string) error {
	return b.SubscribeTickerWithCallBack(accountType, symbol, nil)
}

// 批量订阅ticker
func (b *AsterTickers) SubscribeTickers(accountType AsterAccountType, symbols []string) error {
	return b.SubscribeTickersWithCallBack(accountType, symbols, nil)
}

// 订阅全部ticker
func (b *AsterTickers) SubscribeTickerAll(accountType AsterAccountType) error {
	return b.SubscribeTickerAllWithCallBack(accountType, nil)
}

// 订阅ticker并带上回调
func (b *AsterTickers) SubscribeTickerWithCallBack(accountType AsterAccountType, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.SubscribeTickersWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅ticker并带上回调
func (b *AsterTickers) SubscribeTickersWithCallBack(accountType AsterAccountType, symbols []string, callback func(ticker *Ticker, err error)) error {
	log.Infof("开始订阅Ticker%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentAsterTickersBase *asterTickersBase

	switch accountType {
	case ASTER_SPOT:
		currentAsterTickersBase = b.SpotTickers
	case ASTER_FUTURE:
		currentAsterTickersBase = b.FutureTickers
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentAsterTickersBase.perSubMaxLen
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
			err = currentAsterTickersBase.subscribeAsterTickerMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentAsterTickersBase.WsClientListMap.Load(client)
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
		err = currentAsterTickersBase.subscribeAsterTickerMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("Ticker订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// 订阅全部ticker并带上回调
func (b *AsterTickers) SubscribeTickerAllWithCallBack(accountType AsterAccountType, callback func(tickers []*Ticker, err error)) error {
	log.Infof("开始订阅全部Ticker%s", accountType)

	var currentAsterTickersBase *asterTickersBase

	switch accountType {
	case ASTER_SPOT:
		currentAsterTickersBase = b.SpotTickers
	case ASTER_FUTURE:
		currentAsterTickersBase = b.FutureTickers
	default:
		return ErrorAccountType
	}

	client, err := b.GetCurrentOrNewWsClient(accountType)
	if err != nil {
		return err
	}
	err = currentAsterTickersBase.subscribeAsterTickerAll(client, callback)
	if err != nil {
		return err
	}

	log.Infof("全部Ticker%s订阅结束", accountType)

	return nil
}

func (b *asterTickersBase) Close() {
	b.AsterWsClientBase.close()

	b.TickersMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()
	b.AllTickersSubMap.Clear()
	b.AllTickersCallBackMap.Clear()

}

func (b *AsterTickers) Close() {
	b.SpotTickers.Close()
	b.FutureTickers.Close()
}
