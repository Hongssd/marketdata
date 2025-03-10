package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mybybitapi"
	"strings"
	"sync/atomic"
	"time"
)

type BybitKline struct {
	parent       *BybitMarketData
	SpotKline    *bybitKlineBase
	LinearKline  *bybitKlineBase
	InverseKline *bybitKlineBase
}

type bybitKlineBase struct {
	parent *BybitKline
	BybitWsClientBase
	Exchange    Exchange
	AccountType BybitAccountType
	KlineMap    *MySyncMap[string, *Kline]                                       //symbol_interval->last kline
	WsClientMap *MySyncMap[string, *mybybitapi.PublicWsStreamClient]             //symbol_interval->ws client
	SubMap      *MySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsKline]] //symbol_interval->subscribe
	CallBackMap *MySyncMap[string, func(kline *Kline, err error)]                //symbol_interval->callback
}

func (b *BybitKline) getBaseMapFromAccountType(accountType BybitAccountType) (*bybitKlineBase, error) {
	switch accountType {
	case BYBIT_SPOT:
		return b.SpotKline, nil
	case BYBIT_LINEAR:
		return b.LinearKline, nil
	case BYBIT_INVERSE:
		return b.InverseKline, nil
	}
	return nil, ErrorAccountType
}

func (b *BybitKline) newBybitKlineBase(config BybitKlineConfigBase) *bybitKlineBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 100
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 1000
	}
	return &bybitKlineBase{
		Exchange: BYBIT,
		BybitWsClientBase: BybitWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybybitapi.PublicWsStreamClient, *int64]()),
		},
		KlineMap:    GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybybitapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybybitapi.Subscription[mybybitapi.WsKline]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
}

// 封装好的获取K线方法
func (b *BybitKline) GetLastKline(BybitAccountType BybitAccountType, symbol, interval string) (*Kline, error) {
	symbolKey := symbol + "_" + interval

	bmap, err := b.getBaseMapFromAccountType(BybitAccountType)
	if err != nil {
		return nil, err
	}
	kline, ok := bmap.KlineMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s kline not found", BybitAccountType, symbol)
		return nil, err
	}
	return kline, nil
}

// 获取当前或新建ws客户端
func (b *BybitKline) GetCurrentOrNewWsClient(accountType BybitAccountType) (*mybybitapi.PublicWsStreamClient, error) {
	switch accountType {
	case BYBIT_SPOT:
		return b.SpotKline.GetCurrentOrNewWsClient(accountType)
	case BYBIT_LINEAR:
		return b.LinearKline.GetCurrentOrNewWsClient(accountType)
	case BYBIT_INVERSE:
		return b.InverseKline.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (b *bybitKlineBase) subscribeBybitKline(bybitWsClient *mybybitapi.PublicWsStreamClient, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.subscribeBybitKlineMultiple(bybitWsClient, []string{symbol}, []string{interval}, callback)
}

// 订阅BybitK线底层执行
func (b *bybitKlineBase) subscribeBybitKlineMultiple(bybitWsClient *mybybitapi.PublicWsStreamClient, symbols, intervals []string, callback func(kline *Kline, err error)) error {

	bybitSub, err := bybitWsClient.SubscribeKlineMultiple(symbols, intervals)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		for _, interval := range intervals {
			symbolKey := symbol + "_" + interval
			b.WsClientMap.Store(symbolKey, bybitWsClient)
			b.SubMap.Store(symbolKey, bybitSub)
			b.CallBackMap.Store(symbolKey, callback)
		}
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
				topicSplit := strings.Split(result.Topic, ".")
				if len(topicSplit) != 3 {
					continue
				}
				symbol := topicSplit[2]
				symbolKey := symbol + "_" + result.Interval
				now := time.Now().UnixMilli()
				targetTs := result.Ts + b.parent.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				//保存至Kline
				kline := &Kline{
					Timestamp:            targetTs,
					Exchange:             b.Exchange.String(),
					AccountType:          b.AccountType.String(),
					Symbol:               symbol,
					Interval:             result.Interval,
					StartTime:            result.Start,
					Open:                 stringToFloat64(result.Open),
					High:                 stringToFloat64(result.High),
					Low:                  stringToFloat64(result.Low),
					Close:                stringToFloat64(result.Close),
					Volume:               stringToFloat64(result.Volume),
					CloseTime:            result.End,
					TransactionVolume:    stringToFloat64(result.Turnover),
					TransactionNumber:    0,
					BuyTransactionVolume: 0,
					BuyTransactionAmount: 0,
					Confirm:              result.Confirm,
				}
				b.KlineMap.Store(symbolKey, kline)
				if callback != nil {
					callback(kline, nil)
				}
			case <-bybitSub.CloseChan():
				log.Info("订阅已关闭: ", bybitSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols) * len(intervals))
	count, ok := b.WsClientListMap.Load(bybitWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(bybitWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅BybitK线
func (b *bybitKlineBase) UnSubscribeBybitKline(symbol, interval string) error {
	symbolKey := symbol + "_" + interval
	bybitSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return bybitSub.Unsubscribe()
}

// 订阅K线
func (b *BybitKline) SubscribeKline(accountType BybitAccountType, symbol, interval string) error {
	return b.SubscribeKlineWithCallBack(accountType, symbol, interval, nil)
}

// 批量订阅K线
func (b *BybitKline) SubscribeKlines(accountType BybitAccountType, symbols, intervals []string) error {
	return b.SubscribeKlinesWithCallBack(accountType, symbols, intervals, nil)
}

// 订阅K线并带上回调
func (b *BybitKline) SubscribeKlineWithCallBack(accountType BybitAccountType, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.SubscribeKlinesWithCallBack(accountType, []string{symbol}, []string{interval}, callback)
}

// 批量订阅K线并带上回调
func (b *BybitKline) SubscribeKlinesWithCallBack(accountType BybitAccountType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	log.Infof("开始订阅K线%s，交易对数:%d, 周期数:%d, 总订阅数:%d", accountType, len(symbols), len(intervals), len(symbols)*len(intervals))

	var currentBybitKlineBase *bybitKlineBase

	switch accountType {
	case BYBIT_SPOT:
		currentBybitKlineBase = b.SpotKline
	case BYBIT_LINEAR:
		currentBybitKlineBase = b.LinearKline
	case BYBIT_INVERSE:
		currentBybitKlineBase = b.InverseKline
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentBybitKlineBase.perSubMaxLen
	if len(symbols)*len(intervals) > LEN {
		for i := 0; i < len(symbols); i += LEN / len(intervals) {
			end := i + LEN/len(intervals)
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := b.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentBybitKlineBase.subscribeBybitKlineMultiple(client, tempSymbols, intervals, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBybitKlineBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("K线%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols)*len(intervals), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentBybitKlineBase.subscribeBybitKlineMultiple(client, symbols, intervals, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("K线订阅结束，交易对数:%d, 周期数:%d, 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))

	return nil
}

func (b *bybitKlineBase) Close() {
	b.BybitWsClientBase.close()

	b.KlineMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *BybitKline) Close() {
	b.SpotKline.Close()
	b.LinearKline.Close()
	b.InverseKline.Close()
}
