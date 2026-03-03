package marketdata

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myxcoinapi"
)

type XcoinKline struct {
	parent          *XcoinMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	KlineMap        *MySyncMap[string, *Kline]
	WsClientListMap *MySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myxcoinapi.PublicWsStreamClient]
	SubMap          *MySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsKline]]
	CallBackMap     *MySyncMap[string, func(kline *Kline, err error)]
}

func (xm *XcoinMarketData) newXcoinKline(config XcoinKlineConfig) *XcoinKline {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 100
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 1000
	}
	return &XcoinKline{
		parent:          xm,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        XCOIN,
		KlineMap:        GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myxcoinapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsKline]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
}

func (x *XcoinKline) GetLastKline(businessType XcoinBusinessType, symbol string, interval string) (*Kline, error) {
	symbolKey := symbol + "_" + interval
	kline, ok := x.KlineMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s interval:%s kline not found", symbol, interval)
		return nil, err
	}
	return kline, nil
}

func (x *XcoinKline) GetCurrentOrNewWsClient() (*myxcoinapi.PublicWsStreamClient, error) {
	return x.parent.GetPublicCurrentOrNewWsClient(x.perConnSubNum, x.WsClientListMap)
}

func (x *XcoinKline) SubscribeKline(businessType XcoinBusinessType, symbol, interval string) error {
	return x.SubscribeKlineWithCallBack(businessType, symbol, interval, nil)
}

func (x *XcoinKline) SubscribeKlines(businessType XcoinBusinessType, symbols, intervals []string) error {
	return x.SubscribeKlinesWithCallBack(businessType, symbols, intervals, nil)
}

func (x *XcoinKline) SubscribeKlineWithCallBack(businessType XcoinBusinessType, symbol, interval string, callback func(kline *Kline, err error)) error {
	return x.SubscribeKlinesWithCallBack(businessType, []string{symbol}, []string{interval}, callback)
}

func (x *XcoinKline) SubscribeKlinesWithCallBack(businessType XcoinBusinessType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	log.Infof("开始订阅Kline，交易对数:%d, 周期数:%d 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))
	LEN := x.perSubMaxLen
	if len(symbols)*len(intervals) > LEN {
		for i := 0; i < len(symbols); i += LEN / len(intervals) {
			end := i + LEN/len(intervals)
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := x.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = x.subscribeXcoinKlineMultiple(client, businessType, tempSymbols, intervals, callback)
			if err != nil {
				return err
			}
			currentCount, ok := x.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("K线分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := x.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = x.subscribeXcoinKlineMultiple(client, businessType, symbols, intervals, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("K线订阅结束，交易对数:%d, 周期数:%d, 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))
	return nil
}

// 批量订阅K线并带上回调
func (x *XcoinKline) subscribeXcoinKlineMultiple(client *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	xcoinSub, err := client.SubscribeKlineMulti(businessType.String(), intervals, symbols...)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		for _, interval := range intervals {
			symbolKey := symbol + "_" + interval
			x.WsClientMap.Store(symbolKey, client)
			x.SubMap.Store(symbolKey, xcoinSub)
			x.CallBackMap.Store(symbolKey, callback)
		}
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
				split := strings.Split(result.WsSubscribeArg.Stream, "#")
				if len(split) != 2 {
					log.Warnf("subscribe err, split could be empty")
					continue
				}
				interval := split[1]
				symbolKey := result.WsSubscribeArg.Symbol + "_" + interval
				now := time.Now().UnixMilli()
				targetTs := result.Ts + x.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				//保存至Kline
				kline := &Kline{
					Timestamp:            targetTs,
					Exchange:             x.Exchange.String(),
					AccountType:          businessType.String(),
					Symbol:               result.WsSubscribeArg.Symbol,
					Interval:             interval,
					StartTime:            stringToInt64(result.OpenTime),
					Open:                 stringToFloat64(result.OpenPrice),
					High:                 stringToFloat64(result.HighPrice),
					Low:                  stringToFloat64(result.LowPrice),
					Close:                stringToFloat64(result.ClosePrice),
					Volume:               stringToFloat64(result.Volume),
					CloseTime:            stringToInt64(result.CloseTime),
					TransactionVolume:    stringToFloat64(result.QuoteVolume),
					TransactionNumber:    0,
					BuyTransactionVolume: 0,
					BuyTransactionAmount: 0,
					Confirm:              false,
				}
				x.KlineMap.Store(symbolKey, kline)
				if callback != nil {
					callback(kline, nil)
				}
			case <-xcoinSub.CloseChan():
				log.Info("订阅已关闭: ", xcoinSub.Args)
				return
			}
		}
	}()
	currentCount := int64(len(symbols) * len(intervals))
	count, ok := x.WsClientListMap.Load(client)
	if !ok {
		initCount := int64(0)
		count = &initCount
		x.WsClientListMap.Store(client, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (x *XcoinKline) Close() {
	x.WsClientListMap.Range(func(k *myxcoinapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	x.KlineMap.Clear()
	x.WsClientListMap.Clear()
	x.WsClientMap.Clear()
	x.SubMap.Clear()
	x.CallBackMap.Clear()
}
