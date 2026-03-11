package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myxcoinapi"
	"github.com/shopspring/decimal"
)

type XcoinTickers struct {
	parent          *XcoinMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	TickerMap       *MySyncMap[string, *Ticker]
	WsClientListMap *MySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myxcoinapi.PublicWsStreamClient]
	SubMap          *MySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsTicker24hr]]
	CallBackMap     *MySyncMap[string, func(ticker *Ticker, err error)]
}

func (xm *XcoinMarketData) newXcoinTickers(config XcoinTickersConfig) *XcoinTickers {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &XcoinTickers{
		parent:          xm,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        XCOIN,
		TickerMap:       GetPointer(NewMySyncMap[string, *Ticker]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myxcoinapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsTicker24hr]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(ticker *Ticker, err error)]()),
	}
}

// 封装好的获取ticker方法
func (x *XcoinTickers) GetLastTicker(accountType XcoinBusinessType, symbol string) (*Ticker, error) {
	symbolKey := symbol

	ticker, ok := x.TickerMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s ticker not found", symbol)
		return nil, err
	}
	return ticker, nil
}

func (x *XcoinTickers) GetCurrentOrNewWsClient() (*myxcoinapi.PublicWsStreamClient, error) {
	return x.parent.GetPublicCurrentOrNewWsClient(x.perConnSubNum, x.WsClientListMap)
}

func (x *XcoinTickers) SubscribeTicker(businessType XcoinBusinessType, symbol string) error {
	return x.SubscribeTickerWithCallBack(businessType, symbol, nil)
}

func (x *XcoinTickers) SubscribeTickers(businessType XcoinBusinessType, symbols []string) error {
	return x.SubscribeTickersWithCallBack(businessType, symbols, nil)
}

func (x *XcoinTickers) SubscribeTickerWithCallBack(businessType XcoinBusinessType, symbol string, callback func(ticker *Ticker, err error)) error {
	return x.SubscribeTickersWithCallBack(businessType, []string{symbol}, callback)
}

func (x *XcoinTickers) SubscribeTickersWithCallBack(businessType XcoinBusinessType, symbols []string, callback func(ticker *Ticker, err error)) error {
	log.Infof("开始订阅Tickers，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	//订阅总数超过LEN次，分批订阅
	LEN := x.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := x.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = x.subscribeXcoinTickersMultiple(client, businessType, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := x.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Ticker分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := x.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = x.subscribeXcoinTickersMultiple(client, businessType, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("Ticker订阅结束，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// 订阅Xcoin Tickers
func (x *XcoinTickers) subscribeXcoinTickersMultiple(xcoinWsClient *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbols []string, callback func(ticker *Ticker, err error)) error {
	xcoinSub, err := xcoinWsClient.SubscribeTicker24hrMulti(businessType.String(), symbols...)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		symbolKey := symbol
		x.WsClientMap.Store(symbolKey, xcoinWsClient)
		x.SubMap.Store(symbolKey, xcoinSub)
		x.CallBackMap.Store(symbolKey, callback)
	}
	go func() {
		for {
			select {
			case err := <-xcoinSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-xcoinSub.ResultChan():
				symbolKey := result.WsSubscribeArg.Symbol
				now := time.Now().UnixMilli()
				targetTs := result.Ts + x.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				lastPrice := decimal.RequireFromString(result.LastPrice)
				priceChange := decimal.RequireFromString(result.PriceChange)
				prevPrice := lastPrice.Sub(priceChange).InexactFloat64()
				//保存至Ticker
				ticker := &Ticker{
					Timestamp:       targetTs,
					Exchange:        x.Exchange.String(),
					AccountType:     businessType.String(),
					Symbol:          result.WsSubscribeArg.Symbol,
					LastPrice:       stringToFloat64(result.LastPrice),
					Price24hPercent: stringToFloat64(result.PriceChangePercent),
					PrevPrice24h:    prevPrice,
					HighPrice24h:    stringToFloat64(result.HighPrice),
					LowPrice24h:     stringToFloat64(result.LowPrice),
					Volume24h:       stringToFloat64(result.FillQty),
					Turnover24h:     stringToFloat64(result.FillAmount),
				}
				x.TickerMap.Store(symbolKey, ticker)
				if callback != nil {
					callback(ticker, nil)
				}
			case <-xcoinSub.CloseChan():
				log.Info("订阅已关闭: ", xcoinSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := x.WsClientListMap.Load(xcoinWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		x.WsClientListMap.Store(xcoinWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (x *XcoinTickers) Close() {
	x.WsClientListMap.Range(func(k *myxcoinapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})
	x.TickerMap.Clear()
	x.WsClientListMap.Clear()
	x.WsClientMap.Clear()
	x.SubMap.Clear()
	x.CallBackMap.Clear()
}
