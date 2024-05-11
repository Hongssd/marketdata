package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myokxapi"
)

type OkxKline struct {
	parent          *OkxMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	KlineMap        *MySyncMap[string, *Kline]
	WsClientListMap *MySyncMap[*myokxapi.BusinessWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myokxapi.BusinessWsStreamClient]
	SubMap          *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsCandles]]
	CallBackMap     *MySyncMap[string, func(kline *Kline, err error)]
}

func (om *OkxMarketData) newOkxKline(config OkxKlineConfig) *OkxKline {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 1000
	}
	o := &OkxKline{
		parent:          om,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        OKX,
		KlineMap:        GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myokxapi.BusinessWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myokxapi.BusinessWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsCandles]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
	return o
}

func (o *OkxKline) GetLastKline(symbol string, interval string) (*Kline, error) {
	symbolKey := symbol + "_" + interval
	kline, ok := o.KlineMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s interval:%s kline not found", symbol, interval)
		return nil, err
	}
	return kline, nil
}

func (o *OkxKline) GetCurrentOrNewWsClient() (*myokxapi.BusinessWsStreamClient, error) {
	return o.parent.GetBusinessCurrentOrNewWsClient(o.perConnSubNum, o.WsClientListMap)
}

func (o *OkxKline) SubscribeKline(symbol, interval string) error {
	return o.SubscribeKlineWithCallBack(symbol, interval, nil)
}

func (o *OkxKline) SubscribeKlines(symbols, intervals []string) error {
	return o.SubscribeKlinesWithCallBack(symbols, intervals, nil)
}

func (o *OkxKline) SubscribeKlineWithCallBack(symbol, interval string, callback func(kline *Kline, err error)) error {
	return o.SubscribeKlinesWithCallBack([]string{symbol}, []string{interval}, callback)
}

func (o *OkxKline) SubscribeKlinesWithCallBack(symbols, intervals []string, callback func(kline *Kline, err error)) error {
	log.Infof("开始订阅Kline，交易对数:%d, 周期数:%d 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))
	//订阅总数超过LEN次，分批订阅
	LEN := o.perSubMaxLen
	if len(symbols)*len(intervals) > LEN {
		for i := 0; i < len(symbols); i += LEN / len(intervals) {
			end := i + LEN/len(intervals)
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := o.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = o.subscribeOkxKlineMultiple(client, tempSymbols, intervals, callback)
			if err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("K线分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = o.subscribeOkxKlineMultiple(client, symbols, intervals, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("K线订阅结束，交易对数:%d, 周期数:%d, 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))

	return nil
}

// 订阅OKX深度
func (o *OkxKline) subscribeOkxKlineMultiple(okxWsClient *myokxapi.BusinessWsStreamClient, symbols, intervals []string, callback func(kline *Kline, err error)) error {

	okxSub, err := okxWsClient.SubscribeCandleMultiple(symbols, intervals)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		for _, interval := range intervals {
			symbolKey := symbol + "_" + interval
			o.WsClientMap.Store(symbolKey, okxWsClient)
			o.SubMap.Store(symbolKey, okxSub)
			o.CallBackMap.Store(symbolKey, callback)
		}
	}
	go func() {
		for {
			select {
			case err := <-okxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-okxSub.ResultChan():
				Symbol := result.InstId
				Interval := result.Interval
				symbolKey := Symbol + "_" + Interval

				confirm := false
				if result.Confirm == "1" {
					confirm = true
				}
				accountType := okx_common.GetAccountTypeFromSymbol(result.InstId)
				volume := 0.0
				if accountType == "SPOT" {
					volume = stringToFloat64(result.Vol)
				} else {
					volume = stringToFloat64(result.VolCcy)
				}
				//保存至Kline
				kline := &Kline{
					Timestamp:            time.Now().UnixMilli(),
					Exchange:             o.Exchange.String(),
					AccountType:          accountType,
					Symbol:               result.InstId,
					Interval:             result.Interval,
					StartTime:            stringToInt64(result.Ts),
					Open:                 stringToFloat64(result.O),
					High:                 stringToFloat64(result.H),
					Low:                  stringToFloat64(result.L),
					Close:                stringToFloat64(result.C),
					Volume:               volume,
					CloseTime:            stringToInt64(result.Ts) + OkxInterval(result.Interval).Millisecond() - 1,
					TransactionVolume:    stringToFloat64(result.VolCcyQuote),
					TransactionNumber:    0,
					BuyTransactionVolume: 0,
					BuyTransactionAmount: 0,
					Confirm:              confirm,
				}
				o.KlineMap.Store(symbolKey, kline)
				if callback != nil {
					callback(kline, nil)
				}
			case <-okxSub.CloseChan():
				log.Info("订阅已关闭: ", okxSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols) * len(intervals))
	count, ok := o.WsClientListMap.Load(okxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		o.WsClientListMap.Store(okxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}
