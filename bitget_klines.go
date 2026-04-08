package marketdata

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybitgetapi"
)

type BitgetKline struct {
	parent             *BitgetMarketData
	SpotKline          *bitgetKlineBase
	UsdtFuturesKline   *bitgetKlineBase
	CoinFuturesKline   *bitgetKlineBase
	UsdcFuturesKline   *bitgetKlineBase
}

type bitgetKlineBase struct {
	parent *BitgetKline
	BitgetWsClientBase
	Exchange    Exchange
	AccountType BitgetAccountType
	InstType    mybitgetapi.InstType
	KlineMap    *MySyncMap[string, *Kline]
	WsClientMap *MySyncMap[string, *mybitgetapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsCandle]]
	CallBackMap *MySyncMap[string, func(kline *Kline, err error)]
}

func normalizeBitgetKlineConfigBase(c *BitgetKlineConfigBase) {
	if c.PerSubMaxLen == 0 {
		c.PerSubMaxLen = 100
	}
	if c.PerConnSubNum == 0 {
		c.PerConnSubNum = 100
	}
}

func (bm *BitgetMarketData) newBitgetKline(config BitgetKlineConfig) *BitgetKline {
	normalizeBitgetKlineConfigBase(&config.SpotConfig)
	normalizeBitgetKlineConfigBase(&config.UsdtFuturesConfig)
	normalizeBitgetKlineConfigBase(&config.CoinFuturesConfig)
	normalizeBitgetKlineConfigBase(&config.UsdcFuturesConfig)
	b := &BitgetKline{parent: bm}
	b.SpotKline = b.newBitgetKlineBase(config.SpotConfig, BITGET_SPOT)
	b.SpotKline.parent = b
	b.UsdtFuturesKline = b.newBitgetKlineBase(config.UsdtFuturesConfig, BITGET_USDT_FUTURES)
	b.UsdtFuturesKline.parent = b
	b.CoinFuturesKline = b.newBitgetKlineBase(config.CoinFuturesConfig, BITGET_COIN_FUTURES)
	b.CoinFuturesKline.parent = b
	b.UsdcFuturesKline = b.newBitgetKlineBase(config.UsdcFuturesConfig, BITGET_USDC_FUTURES)
	b.UsdcFuturesKline.parent = b
	return b
}

func (b *BitgetKline) newBitgetKlineBase(config BitgetKlineConfigBase, accountType BitgetAccountType) *bitgetKlineBase {
	instType, _ := BitgetAccountTypeToInstType(accountType)
	return &bitgetKlineBase{
		BitgetWsClientBase: BitgetWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybitgetapi.PublicWsStreamClient, *int64]()),
		},
		Exchange:    BITGET,
		AccountType: accountType,
		InstType:    instType,
		KlineMap:    GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybitgetapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsCandle]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
}

// bitgetIntervalToWsCandle 将常用周期字符串转为 SDK 枚举；其余按 Bitget WS 原样透传。
func bitgetIntervalToWsCandle(interval string) mybitgetapi.WsCandleInterval {
	s := strings.TrimSpace(interval)
	switch strings.ToLower(s) {
	case "1m":
		return mybitgetapi.WS_CANDLE_1M
	case "3m":
		return mybitgetapi.WS_CANDLE_3M
	case "5m":
		return mybitgetapi.WS_CANDLE_5M
	case "15m":
		return mybitgetapi.WS_CANDLE_15M
	case "30m":
		return mybitgetapi.WS_CANDLE_30M
	case "1h", "1H":
		return mybitgetapi.WS_CANDLE_1H
	case "4h", "4H":
		return mybitgetapi.WS_CANDLE_4H
	case "6h", "6H":
		return mybitgetapi.WS_CANDLE_6H
	case "12h", "12H":
		return mybitgetapi.WS_CANDLE_12H
	case "1d", "1D":
		return mybitgetapi.WS_CANDLE_1D
	default:
		return mybitgetapi.WsCandleInterval(s)
	}
}

func (b *BitgetKline) getBaseFromAccountType(accountType BitgetAccountType) (*bitgetKlineBase, error) {
	switch accountType {
	case BITGET_SPOT:
		return b.SpotKline, nil
	case BITGET_USDT_FUTURES:
		return b.UsdtFuturesKline, nil
	case BITGET_COIN_FUTURES:
		return b.CoinFuturesKline, nil
	case BITGET_USDC_FUTURES:
		return b.UsdcFuturesKline, nil
	default:
		return nil, ErrorAccountType
	}
}

func (b *BitgetKline) GetLastKline(accountType BitgetAccountType, symbol, interval string) (*Kline, error) {
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return nil, err
	}
	key := symbol + "_" + interval
	k, ok := base.KlineMap.Load(key)
	if !ok {
		return nil, fmt.Errorf("%s symbol:%s interval:%s kline not found", accountType, symbol, interval)
	}
	return k, nil
}

func (k *bitgetKlineBase) subscribeBitgetKlineMultiple(wsClient *mybitgetapi.PublicWsStreamClient, symbols []string, wsInterval mybitgetapi.WsCandleInterval, intervalKey string, callback func(kline *Kline, err error)) error {
	sub, err := wsClient.SubscribeCandlesMultiple(k.InstType, symbols, wsInterval)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		mapKey := symbol + "_" + intervalKey
		k.WsClientMap.Store(mapKey, wsClient)
		k.SubMap.Store(mapKey, sub)
		k.CallBackMap.Store(mapKey, callback)
	}
	go func() {
		for {
			select {
			case err := <-sub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-sub.ResultChan():
				if len(result.Data) == 0 {
					continue
				}
				sym := result.Arg.Symbol
				iv := result.Arg.Interval
				if iv == "" {
					iv = intervalKey
				}
				mapKey := sym + "_" + iv
				row := result.Data[len(result.Data)-1]
				now := time.Now().UnixMilli()
				targetTs := result.Ts + k.parent.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				startMs := stringToInt64(row.Start)
				kl := &Kline{
					Timestamp:         targetTs,
					Exchange:          k.Exchange.String(),
					AccountType:       k.AccountType.String(),
					Symbol:            sym,
					Interval:          iv,
					StartTime:         startMs,
					Open:              stringToFloat64(row.Open),
					High:              stringToFloat64(row.High),
					Low:               stringToFloat64(row.Low),
					Close:             stringToFloat64(row.Close),
					Volume:            stringToFloat64(row.Volume),
					TransactionVolume: stringToFloat64(row.Turnover),
					Confirm:           strings.EqualFold(result.Action, "update"),
				}
				k.KlineMap.Store(mapKey, kl)
				cb, _ := k.CallBackMap.Load(mapKey)
				if cb != nil {
					cb(kl, nil)
				}
			case <-sub.CloseChan():
				log.Info("Bitget K线订阅已关闭: ", sub.Args)
				return
			}
		}
	}()
	currentCount := int64(len(symbols))
	count, ok := k.WsClientListMap.Load(wsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		k.WsClientListMap.Store(wsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (b *BitgetKline) SubscribeKline(accountType BitgetAccountType, symbol, interval string) error {
	return b.SubscribeKlineWithCallBack(accountType, symbol, interval, nil)
}

func (b *BitgetKline) SubscribeKlines(accountType BitgetAccountType, symbols, intervals []string) error {
	return b.SubscribeKlinesWithCallBack(accountType, symbols, intervals, nil)
}

func (b *BitgetKline) SubscribeKlineWithCallBack(accountType BitgetAccountType, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.SubscribeKlinesWithCallBack(accountType, []string{symbol}, []string{interval}, callback)
}

func (b *BitgetKline) SubscribeKlinesWithCallBack(accountType BitgetAccountType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return err
	}
	log.Infof("开始订阅 Bitget K线，accountType:%s，交易对:%d，周期:%d", accountType, len(symbols), len(intervals))
	LEN := base.perSubMaxLen
	for _, interval := range intervals {
		wsIv := bitgetIntervalToWsCandle(interval)
		if len(symbols) > LEN {
			for i := 0; i < len(symbols); i += LEN {
				end := i + LEN
				if end > len(symbols) {
					end = len(symbols)
				}
				batch := symbols[i:end]
				client, err := base.GetCurrentOrNewWsClient()
				if err != nil {
					return err
				}
				if err = base.subscribeBitgetKlineMultiple(client, batch, wsIv, interval, callback); err != nil {
					return err
				}
				currentCount, ok := base.WsClientListMap.Load(client)
				if !ok {
					return errors.New("WsClientListMap Load error")
				}
				log.Infof("Bitget K线分批订阅 interval=%s，本批:%v，链接订阅数:%d，等待1秒...", interval, batch, *currentCount)
				time.Sleep(time.Second)
			}
		} else {
			client, err := base.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			if err = base.subscribeBitgetKlineMultiple(client, symbols, wsIv, interval, callback); err != nil {
				return err
			}
		}
	}
	log.Infof("Bitget K线订阅结束，accountType:%s", accountType)
	return nil
}

func (k *bitgetKlineBase) Close() {
	k.BitgetWsClientBase.close()
	k.KlineMap.Clear()
	k.WsClientMap.Clear()
	k.SubMap.Clear()
	k.CallBackMap.Clear()
}

func (b *BitgetKline) Close() {
	b.SpotKline.Close()
	b.UsdtFuturesKline.Close()
	b.CoinFuturesKline.Close()
	b.UsdcFuturesKline.Close()
}
