package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybitgetapi"
	"github.com/shopspring/decimal"
)

type BitgetTickers struct {
	parent             *BitgetMarketData
	SpotTickers        *bitgetTickersBase
	UsdtFuturesTickers *bitgetTickersBase
	CoinFuturesTickers *bitgetTickersBase
	UsdcFuturesTickers *bitgetTickersBase
}

type bitgetTickersBase struct {
	parent *BitgetTickers
	BitgetWsClientBase
	Exchange    Exchange
	AccountType BitgetAccountType
	InstType    mybitgetapi.InstType
	TickerMap   *MySyncMap[string, *Ticker]
	WsClientMap *MySyncMap[string, *mybitgetapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsTicker]]
	CallBackMap *MySyncMap[string, func(ticker *Ticker, err error)]
}

func normalizeBitgetTickersConfigBase(c *BitgetTickersConfigBase) {
	if c.PerSubMaxLen == 0 {
		c.PerSubMaxLen = 100
	}
	if c.PerConnSubNum == 0 {
		c.PerConnSubNum = 100
	}
}

func (bm *BitgetMarketData) newBitgetTickers(config BitgetTickersConfig) *BitgetTickers {
	normalizeBitgetTickersConfigBase(&config.SpotConfig)
	normalizeBitgetTickersConfigBase(&config.UsdtFuturesConfig)
	normalizeBitgetTickersConfigBase(&config.CoinFuturesConfig)
	normalizeBitgetTickersConfigBase(&config.UsdcFuturesConfig)
	b := &BitgetTickers{parent: bm}
	b.SpotTickers = b.newBitgetTickersBase(config.SpotConfig, BITGET_SPOT)
	b.SpotTickers.parent = b
	b.UsdtFuturesTickers = b.newBitgetTickersBase(config.UsdtFuturesConfig, BITGET_USDT_FUTURES)
	b.UsdtFuturesTickers.parent = b
	b.CoinFuturesTickers = b.newBitgetTickersBase(config.CoinFuturesConfig, BITGET_COIN_FUTURES)
	b.CoinFuturesTickers.parent = b
	b.UsdcFuturesTickers = b.newBitgetTickersBase(config.UsdcFuturesConfig, BITGET_USDC_FUTURES)
	b.UsdcFuturesTickers.parent = b
	return b
}

func (b *BitgetTickers) newBitgetTickersBase(config BitgetTickersConfigBase, accountType BitgetAccountType) *bitgetTickersBase {
	instType, _ := BitgetAccountTypeToInstType(accountType)
	return &bitgetTickersBase{
		BitgetWsClientBase: BitgetWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybitgetapi.PublicWsStreamClient, *int64]()),
		},
		Exchange:    BITGET,
		AccountType: accountType,
		InstType:    instType,
		TickerMap:   GetPointer(NewMySyncMap[string, *Ticker]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybitgetapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsTicker]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(ticker *Ticker, err error)]()),
	}
}

func (b *BitgetTickers) getBaseFromAccountType(accountType BitgetAccountType) (*bitgetTickersBase, error) {
	switch accountType {
	case BITGET_SPOT:
		return b.SpotTickers, nil
	case BITGET_USDT_FUTURES:
		return b.UsdtFuturesTickers, nil
	case BITGET_COIN_FUTURES:
		return b.CoinFuturesTickers, nil
	case BITGET_USDC_FUTURES:
		return b.UsdcFuturesTickers, nil
	default:
		return nil, ErrorAccountType
	}
}

func (b *BitgetTickers) GetLastTicker(accountType BitgetAccountType, symbol string) (*Ticker, error) {
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return nil, err
	}
	ticker, ok := base.TickerMap.Load(symbol)
	if !ok {
		return nil, fmt.Errorf("%s symbol:%s ticker not found", accountType, symbol)
	}
	return ticker, nil
}

func (b *bitgetTickersBase) subscribeBitgetTickersMultiple(wsClient *mybitgetapi.PublicWsStreamClient, symbols []string, callback func(ticker *Ticker, err error)) error {
	bitgetSub, err := wsClient.SubscribeTickersMultiple(b.InstType, symbols)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, wsClient)
		b.SubMap.Store(symbolKey, bitgetSub)
		b.CallBackMap.Store(symbolKey, callback)
	}
	go func() {
		for {
			select {
			case err := <-bitgetSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-bitgetSub.ResultChan():
				if len(result.Data) == 0 {
					continue
				}
				symbolKey := result.Arg.Symbol
				for _, row := range result.Data {
					now := time.Now().UnixMilli()
					targetTs := result.Ts + b.parent.parent.GetServerTimeDelta()
					if targetTs > now {
						targetTs = now
					}
					lastPrice, _ := decimal.NewFromString(row.LastPrice)
					open24h, _ := decimal.NewFromString(row.OpenPrice24h)
					var price24hPercent float64
					if !open24h.IsZero() {
						priceChange := lastPrice.Sub(open24h)
						price24hPercent, _ = priceChange.Div(open24h).Mul(decimal.NewFromInt(100)).Float64()
					}
					ticker := &Ticker{
						Timestamp:       targetTs,
						Exchange:        b.Exchange.String(),
						AccountType:     b.AccountType.String(),
						Symbol:          symbolKey,
						LastPrice:       stringToFloat64(row.LastPrice),
						Price24hPercent: price24hPercent,
						PrevPrice24h:    stringToFloat64(row.OpenPrice24h),
						HighPrice24h:    stringToFloat64(row.HighPrice24h),
						LowPrice24h:     stringToFloat64(row.LowPrice24h),
						Volume24h:       stringToFloat64(row.Volume24h),
						Turnover24h:     stringToFloat64(row.Turnover24h),
					}
					b.TickerMap.Store(symbolKey, ticker)
					cb, _ := b.CallBackMap.Load(symbolKey)
					if cb != nil {
						cb(ticker, nil)
					}
				}
			case <-bitgetSub.CloseChan():
				log.Info("订阅已关闭: ", bitgetSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(wsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(wsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (b *BitgetTickers) SubscribeTicker(accountType BitgetAccountType, symbol string) error {
	return b.SubscribeTickerWithCallBack(accountType, symbol, nil)
}

func (b *BitgetTickers) SubscribeTickers(accountType BitgetAccountType, symbols []string) error {
	return b.SubscribeTickersWithCallBack(accountType, symbols, nil)
}

func (b *BitgetTickers) SubscribeTickerWithCallBack(accountType BitgetAccountType, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.SubscribeTickersWithCallBack(accountType, []string{symbol}, callback)
}

func (b *BitgetTickers) SubscribeTickersWithCallBack(accountType BitgetAccountType, symbols []string, callback func(ticker *Ticker, err error)) error {
	log.Infof("开始订阅Bitget Tickers，accountType:%s，交易对数:%d", accountType, len(symbols))
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return err
	}
	LEN := base.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := base.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = base.subscribeBitgetTickersMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := base.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Bitget Ticker分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := base.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = base.subscribeBitgetTickersMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}
	log.Infof("Bitget Ticker订阅结束，accountType:%s，交易对数:%d", accountType, len(symbols))
	return nil
}

func (b *bitgetTickersBase) Close() {
	b.BitgetWsClientBase.close()
	b.TickerMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()
}

func (b *BitgetTickers) Close() {
	b.SpotTickers.Close()
	b.UsdtFuturesTickers.Close()
	b.CoinFuturesTickers.Close()
	b.UsdcFuturesTickers.Close()
}
