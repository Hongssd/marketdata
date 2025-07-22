package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybybitapi"
)

type BybitTickers struct {
	parent         *BybitMarketData
	SpotTickers    *bybitTickersBase
	LinearTickers  *bybitTickersBase
	InverseTickers *bybitTickersBase
	OptionTickers  *bybitTickersBase
}

type bybitTickersBase struct {
	parent *BybitTickers
	BybitWsClientBase
	Exchange    Exchange
	AccountType BybitAccountType
	TickerMap   *MySyncMap[string, *Ticker]                                       //symbol->last ticker
	WsClientMap *MySyncMap[string, *mybybitapi.PublicWsStreamClient]              //symbol->ws client
	SubMap      *MySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsTicker]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(ticker *Ticker, err error)]               //symbol->callback
}

func (b *BybitTickers) getBaseMapFromAccountType(accountType BybitAccountType) (*bybitTickersBase, error) {
	switch accountType {
	case BYBIT_SPOT:
		return b.SpotTickers, nil
	case BYBIT_LINEAR:
		return b.LinearTickers, nil
	case BYBIT_INVERSE:
		return b.InverseTickers, nil
	case BYBIT_OPTION:
		return b.OptionTickers, nil
	}
	return nil, ErrorAccountType
}

func (b *BybitTickers) newBybitTickersBase(config BybitTickersConfigBase) *bybitTickersBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &bybitTickersBase{
		Exchange: BYBIT,
		BybitWsClientBase: BybitWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybybitapi.PublicWsStreamClient, *int64]()),
		},
		TickerMap:   GetPointer(NewMySyncMap[string, *Ticker]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybybitapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsTicker]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(ticker *Ticker, err error)]()),
	}
}

// 封装好的获取ticker方法
func (b *BybitTickers) GetLastTicker(accountType BybitAccountType, symbol string) (*Ticker, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(accountType)
	if err != nil {
		return nil, err
	}
	ticker, ok := bmap.TickerMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s ticker not found", accountType, symbol)
		return nil, err
	}
	return ticker, nil
}

func (b *BybitTickers) GetCurrentOrNewWsClient(accountType BybitAccountType) (*mybybitapi.PublicWsStreamClient, error) {

	switch accountType {
	case BYBIT_SPOT:
		return b.SpotTickers.GetCurrentOrNewWsClient(accountType)
	case BYBIT_LINEAR:
		return b.LinearTickers.GetCurrentOrNewWsClient(accountType)
	case BYBIT_INVERSE:
		return b.InverseTickers.GetCurrentOrNewWsClient(accountType)
	case BYBIT_OPTION:
		return b.OptionTickers.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}

}

// 订阅Bybit ticker底层执行
func (b *bybitTickersBase) subscribeBybitTickerMultiple(bybitWsClient *mybybitapi.PublicWsStreamClient, symbols []string, callback func(ticker *Ticker, err error)) error {

	bybitSub, err := bybitWsClient.SubscribeTickerMultiple(symbols)
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
				symbolKey := result.Symbol
				now := time.Now().UnixMilli()
				targetTs := result.Ts + b.parent.parent.GetServerTimeDelta()
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
					Price24hPercent: stringToFloat64(result.Price24hPcnt),
					PrevPrice24h:    stringToFloat64(result.PrevPrice24h),
					HighPrice24h:    stringToFloat64(result.HighPrice24h),
					LowPrice24h:     stringToFloat64(result.LowPrice24h),
					Volume24h:       stringToFloat64(result.Volume24h),
					Turnover24h:     stringToFloat64(result.Turnover24h),
				}
				b.TickerMap.Store(symbolKey, ticker)
				if callback != nil {
					callback(ticker, nil)
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

// 取消订阅Bybit ticker
func (b *bybitTickersBase) UnSubscribeBybitTicker(symbol string) error {
	symbolKey := symbol
	bybitSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return bybitSub.Unsubscribe()
}

// 订阅ticker
func (b *BybitTickers) SubscribeTicker(accountType BybitAccountType, symbol string) error {
	return b.SubscribeTickerWithCallBack(accountType, symbol, nil)
}

// 批量订阅ticker
func (b *BybitTickers) SubscribeTickers(accountType BybitAccountType, symbols []string) error {
	return b.SubscribeTickersWithCallBack(accountType, symbols, nil)
}

// 订阅ticker并带上回调
func (b *BybitTickers) SubscribeTickerWithCallBack(accountType BybitAccountType, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.SubscribeTickersWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅ticker并带上回调
func (b *BybitTickers) SubscribeTickersWithCallBack(accountType BybitAccountType, symbols []string, callback func(ticker *Ticker, err error)) error {
	log.Infof("开始订阅ticker%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBybitTickersBase *bybitTickersBase

	switch accountType {
	case BYBIT_SPOT:
		currentBybitTickersBase = b.SpotTickers
	case BYBIT_LINEAR:
		currentBybitTickersBase = b.LinearTickers
	case BYBIT_INVERSE:
		currentBybitTickersBase = b.InverseTickers
	case BYBIT_OPTION:
		currentBybitTickersBase = b.OptionTickers
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentBybitTickersBase.perSubMaxLen
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
			err = currentBybitTickersBase.subscribeBybitTickerMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBybitTickersBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("ticker%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentBybitTickersBase.subscribeBybitTickerMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("ticker订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *bybitTickersBase) Close() {
	b.BybitWsClientBase.close()

	b.TickerMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *BybitTickers) Close() {
	b.SpotTickers.Close()
	b.LinearTickers.Close()
	b.InverseTickers.Close()
	b.OptionTickers.Close()
}
