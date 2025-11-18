package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myasterapi"
)

type AsterKline struct {
	parent      *AsterMarketData
	SpotKline   *asterKlineBase
	FutureKline *asterKlineBase
}

type asterKlineBase struct {
	parent *AsterKline
	AsterWsClientBase
	Exchange    Exchange
	AccountType AsterAccountType
	KlineMap    *MySyncMap[string, *Kline]                                       //symbol_interval->last kline
	WsClientMap *MySyncMap[string, *myasterapi.WsStreamClient]                   //symbol_interval->ws client
	SubMap      *MySyncMap[string, *myasterapi.Subscription[myasterapi.WsKline]] //symbol_interval->subscribe
	CallBackMap *MySyncMap[string, func(kline *Kline, err error)]                //symbol_interval->callback
}

func (b *AsterKline) getBaseMapFromAccountType(accountType AsterAccountType) (*asterKlineBase, error) {
	switch accountType {
	case ASTER_SPOT:
		return b.SpotKline, nil
	case ASTER_FUTURE:
		return b.FutureKline, nil
	}
	return nil, ErrorAccountType
}

func (b *AsterKline) newAsterKlineBase(config AsterKlineConfigBase) *asterKlineBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 100
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 1000
	}
	return &asterKlineBase{
		Exchange: ASTER,
		AsterWsClientBase: AsterWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*myasterapi.WsStreamClient, *int64]()),
		},
		KlineMap:    GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *myasterapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *myasterapi.Subscription[myasterapi.WsKline]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
}

// 封装好的获取K线方法
func (b *AsterKline) GetLastKline(AsterAccountType AsterAccountType, symbol, interval string) (*Kline, error) {
	symbolKey := symbol + "_" + interval

	bmap, err := b.getBaseMapFromAccountType(AsterAccountType)
	if err != nil {
		return nil, err
	}
	kline, ok := bmap.KlineMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s kline not found", AsterAccountType, symbol)
		return nil, err
	}
	return kline, nil
}

// 获取当前或新建ws客户端
func (b *AsterKline) GetCurrentOrNewWsClient(accountType AsterAccountType) (*myasterapi.WsStreamClient, error) {
	switch accountType {
	case ASTER_SPOT:
		return b.SpotKline.GetCurrentOrNewWsClient(accountType)
	case ASTER_FUTURE:
		return b.FutureKline.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (b *asterKlineBase) subscribeAsterKline(asterWsClient *myasterapi.WsStreamClient, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.subscribeAsterKlineMultiple(asterWsClient, []string{symbol}, []string{interval}, callback)
}

// 订阅币安K线底层执行
func (b *asterKlineBase) subscribeAsterKlineMultiple(asterWsClient *myasterapi.WsStreamClient, symbols, intervals []string, callback func(kline *Kline, err error)) error {

	asterSub, err := asterWsClient.SubscribeKlineMultiple(symbols, intervals)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		for _, interval := range intervals {
			symbolKey := symbol + "_" + interval
			b.WsClientMap.Store(symbolKey, asterWsClient)
			b.SubMap.Store(symbolKey, asterSub)
			b.CallBackMap.Store(symbolKey, callback)
		}
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
				symbolKey := result.Symbol + "_" + result.Interval
				now := time.Now().UnixMilli()
				targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType)
				if targetTs > now {
					targetTs = now
				}
				//保存至Kline
				kline := &Kline{
					Timestamp:            targetTs,
					Exchange:             b.Exchange.String(),
					AccountType:          b.AccountType.String(),
					Symbol:               result.Symbol,
					Interval:             result.Interval,
					StartTime:            result.StartTime,
					Open:                 result.Open,
					High:                 result.High,
					Low:                  result.Low,
					Close:                result.Close,
					Volume:               result.Volume,
					CloseTime:            result.CloseTime,
					TransactionVolume:    result.TransactionVolume,
					TransactionNumber:    result.TransactionNumber,
					BuyTransactionVolume: result.BuyTransactionVolume,
					BuyTransactionAmount: result.BuyTransactionAmount,
					Confirm:              result.Confirm,
				}
				b.KlineMap.Store(symbolKey, kline)
				if callback != nil {
					callback(kline, nil)
				}
			case <-asterSub.CloseChan():
				log.Info("订阅已关闭: ", asterSub.Params)
				return
			}
		}
	}()

	currentCount := int64(len(symbols) * len(intervals))
	count, ok := b.WsClientListMap.Load(asterWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(asterWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安K线
func (b *asterKlineBase) UnSubscribeAsterKline(symbol, interval string) error {
	symbolKey := symbol + "_" + interval
	asterSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return asterSub.Unsubscribe()
}

// 订阅K线
func (b *AsterKline) SubscribeKline(accountType AsterAccountType, symbol, interval string) error {
	return b.SubscribeKlineWithCallBack(accountType, symbol, interval, nil)
}

// 批量订阅K线
func (b *AsterKline) SubscribeKlines(accountType AsterAccountType, symbols, intervals []string) error {
	return b.SubscribeKlinesWithCallBack(accountType, symbols, intervals, nil)
}

// 订阅K线并带上回调
func (b *AsterKline) SubscribeKlineWithCallBack(accountType AsterAccountType, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.SubscribeKlinesWithCallBack(accountType, []string{symbol}, []string{interval}, callback)
}

// 批量订阅K线并带上回调
func (b *AsterKline) SubscribeKlinesWithCallBack(accountType AsterAccountType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	log.Infof("开始订阅K线%s，交易对数:%d, 周期数:%d, 总订阅数:%d", accountType, len(symbols), len(intervals), len(symbols)*len(intervals))

	var currentAsterKlineBase *asterKlineBase

	switch accountType {
	case ASTER_SPOT:
		currentAsterKlineBase = b.SpotKline
	case ASTER_FUTURE:
		currentAsterKlineBase = b.FutureKline
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentAsterKlineBase.perSubMaxLen
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
			err = currentAsterKlineBase.subscribeAsterKlineMultiple(client, tempSymbols, intervals, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentAsterKlineBase.WsClientListMap.Load(client)
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
		err = currentAsterKlineBase.subscribeAsterKlineMultiple(client, symbols, intervals, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("K线订阅结束，交易对数:%d, 周期数:%d, 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))

	return nil
}

func (b *asterKlineBase) Close() {

	b.AsterWsClientBase.close()

	b.KlineMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *AsterKline) Close() {
	b.SpotKline.Close()
	b.FutureKline.Close()

}
