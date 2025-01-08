package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mygateapi"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type GateKline struct {
	parent        *GateMarketData
	SpotKline     *gateKlineBase
	FuturesKline  *gateKlineBase
	DeliveryKline *gateKlineBase
}

type gateKlineBase struct {
	parent *GateKline
	GateWsClientBase
	Exchange    Exchange
	AccountType GateAccountType
	KlineMap    *MySyncMap[string, *Kline]                                                                            //symbol_interval->last kline
	WsClientMap *MySyncMap[string, *mygateapi.WsStreamClient]                                                         //symbol_interval->ws client
	SubMap      *MySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsCandles]]] //symbol_interval->subscribe
	CallBackMap *MySyncMap[string, func(kline *Kline, err error)]                                                     //symbol_interval->callback
}

func (b *GateKline) getBaseMapFromAccountType(accountType GateAccountType) (*gateKlineBase, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotKline, nil
	case GATE_FUTURES:
		return b.FuturesKline, nil
	case GATE_DELIVERY:
		return b.DeliveryKline, nil
	}
	return nil, ErrorAccountType
}

func (b *GateKline) newGateKlineBase(config GateKlineConfigBase) *gateKlineBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 100
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 1000
	}
	return &gateKlineBase{
		Exchange: GATE,
		GateWsClientBase: GateWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mygateapi.WsStreamClient, *int64]()),
		},
		KlineMap:    GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mygateapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsCandles]]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
}

// 封装好的获取K线方法
func (b *GateKline) GetLastKline(GateAccountType GateAccountType, symbol, interval string) (*Kline, error) {
	symbolKey := symbol + "_" + interval

	bmap, err := b.getBaseMapFromAccountType(GateAccountType)
	if err != nil {
		return nil, err
	}
	kline, ok := bmap.KlineMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s kline not found", GateAccountType, symbol)
		return nil, err
	}
	return kline, nil
}

// 获取当前或新建ws客户端
func (b *GateKline) GetCurrentOrNewWsClient(accountType GateAccountType) (*mygateapi.WsStreamClient, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotKline.GetCurrentOrNewWsClient(accountType)
	case GATE_FUTURES:
		return b.FuturesKline.GetCurrentOrNewWsClient(accountType)
	case GATE_DELIVERY:
		return b.DeliveryKline.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (b *gateKlineBase) subscribeGateKline(gateWsClient *mygateapi.WsStreamClient, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.subscribeGateKlineMultiple(gateWsClient, []string{symbol}, []string{interval}, callback)
}

// 订阅GateK线底层执行
func (b *gateKlineBase) subscribeGateKlineMultiple(gateWsClient *mygateapi.WsStreamClient, symbols, intervals []string, callback func(kline *Kline, err error)) error {

	gateSub, err := gateWsClient.SubscribeCandleMultiple(symbols, intervals)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		for _, interval := range intervals {
			symbolKey := symbol + "_" + interval
			b.WsClientMap.Store(symbolKey, gateWsClient)
			b.SubMap.Store(symbolKey, gateSub)
			b.CallBackMap.Store(symbolKey, callback)
		}
	}

	go func() {
		for {
			select {
			case err := <-gateSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-gateSub.ResultChan():
				result := r.Result
				topicSplit := strings.SplitN(result.Interval, "_", 2)
				if len(topicSplit) != 2 {
					continue
				}
				interval := topicSplit[0]
				symbol := topicSplit[1]
				symbolKey := result.Interval
				timestampUnix, err := strconv.ParseInt(result.Timestamp, 10, 64)
				if err != nil {
					continue
				}
				timestamp := timestampUnix * 1000
				//保存至Kline
				kline := &Kline{
					Timestamp:            r.TimeMs + b.parent.parent.GetServerTimeDelta(),
					Exchange:             b.Exchange.String(),
					AccountType:          b.AccountType.String(),
					Symbol:               symbol,
					Interval:             interval,
					StartTime:            timestamp,
					Open:                 stringToFloat64(result.Open),
					High:                 stringToFloat64(result.High),
					Low:                  stringToFloat64(result.Low),
					Close:                stringToFloat64(result.Close),
					Volume:               stringToFloat64(result.Volume),
					CloseTime:            timestamp + GateInterval(result.Interval).Millisecond() - 1,
					TransactionVolume:    stringToFloat64(result.Amount),
					TransactionNumber:    0,
					BuyTransactionVolume: 0,
					BuyTransactionAmount: 0,
					Confirm:              result.WindowClose,
				}
				b.KlineMap.Store(symbolKey, kline)
				if callback != nil {
					callback(kline, nil)
				}
			case <-gateSub.CloseChan():
				log.Info("订阅已关闭: ", gateSub.SubKeys)
				return
			}
		}
	}()

	currentCount := int64(len(symbols) * len(intervals))
	count, ok := b.WsClientListMap.Load(gateWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(gateWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅GateK线
func (b *gateKlineBase) UnSubscribeGateKline(symbol, interval string) error {
	symbolKey := symbol + "_" + interval
	gateSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return gateSub.Unsubscribe()
}

// 订阅K线
func (b *GateKline) SubscribeKline(accountType GateAccountType, symbol, interval string) error {
	return b.SubscribeKlineWithCallBack(accountType, symbol, interval, nil)
}

// 批量订阅K线
func (b *GateKline) SubscribeKlines(accountType GateAccountType, symbols, intervals []string) error {
	return b.SubscribeKlinesWithCallBack(accountType, symbols, intervals, nil)
}

// 订阅K线并带上回调
func (b *GateKline) SubscribeKlineWithCallBack(accountType GateAccountType, symbol, interval string, callback func(kline *Kline, err error)) error {
	return b.SubscribeKlinesWithCallBack(accountType, []string{symbol}, []string{interval}, callback)
}

// 批量订阅K线并带上回调
func (b *GateKline) SubscribeKlinesWithCallBack(accountType GateAccountType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	log.Infof("开始订阅K线%s，交易对数:%d, 周期数:%d, 总订阅数:%d", accountType, len(symbols), len(intervals), len(symbols)*len(intervals))

	var currentGateKlineBase *gateKlineBase

	switch accountType {
	case GATE_SPOT:
		currentGateKlineBase = b.SpotKline
	case GATE_FUTURES:
		currentGateKlineBase = b.FuturesKline
	case GATE_DELIVERY:
		currentGateKlineBase = b.DeliveryKline
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentGateKlineBase.perSubMaxLen
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
			err = currentGateKlineBase.subscribeGateKlineMultiple(client, tempSymbols, intervals, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentGateKlineBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("K线%s分批订阅成功，此次订阅交易对:%v, 此次订阅周期:%s, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, intervals, len(tempSymbols)*len(intervals), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentGateKlineBase.subscribeGateKlineMultiple(client, symbols, intervals, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("K线订阅结束，交易对数:%d, 周期数:%d, 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))

	return nil
}

func (b *gateKlineBase) Close() {
	b.GateWsClientBase.close()

	b.KlineMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *GateKline) Close() {
	b.SpotKline.Close()
	b.FuturesKline.Close()
	b.DeliveryKline.Close()
}
