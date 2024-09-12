package marketdata

import (
	"fmt"
	"github.com/Hongssd/myokxapi"
	"sync"
	"sync/atomic"
	"time"
)

type OkxMarkPrice struct {
	parent          *OkxMarketData
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	MarkPriceMap    *MySyncMap[string, *MarkPrice]
	WsClientListMap *MySyncMap[*myokxapi.PublicWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myokxapi.PublicWsStreamClient]
	SubMap          *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsMarkPrice]]
	CallBackMap     *MySyncMap[string, func(markPrice *MarkPrice, err error)]
}

func (om *OkxMarketData) newOkxMarkPrice(config OkxMarkPriceConfig) *OkxMarkPrice {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 1000
	}
	o := &OkxMarkPrice{
		parent:          om,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        "OKX",
		MarkPriceMap:    GetPointer(NewMySyncMap[string, *MarkPrice]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myokxapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myokxapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsMarkPrice]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(mp *MarkPrice, err error)]()),
	}

	return o
}

func (o *OkxMarkPrice) GetLastMarkPrice(symbol string) (*MarkPrice, error) {
	mp, ok := o.MarkPriceMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s mark price not found", symbol)
		return nil, err
	}
	return mp, nil
}

func (o *OkxMarkPrice) GetCurrentOrNewWsClient() (*myokxapi.PublicWsStreamClient, error) {
	return o.parent.GetPublicCurrentOrNewWsClient(o.perConnSubNum, o.WsClientListMap)
}

func (o *OkxMarkPrice) SubscribeMarkPrice(symbol string) error {
	return o.SubscribeMarkPriceWithCallBack(symbol, nil)
}

func (o *OkxMarkPrice) SubscribeMarkPrices(symbols []string) error {
	return o.SubscribeMarkPricesWithCallBack(symbols, nil)
}

func (o *OkxMarkPrice) SubscribeMarkPriceWithCallBack(symbol string, callBack func(markPrice *MarkPrice, err error)) error {
	return o.SubscribeMarkPricesWithCallBack([]string{symbol}, callBack)
}

func (o *OkxMarkPrice) SubscribeMarkPricesWithCallBack(symbols []string, callBack func(markPrice *MarkPrice, err error)) error {
	log.Infof("开始订阅MarkPrice，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	//订阅总数超过限制，分批订阅
	LEN := o.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := o.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = o.subscribeOkxMarkPriceMultiple(client, tempSymbols, callBack)
			if err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return fmt.Errorf("WsClientListMap Load failed")
			}
			log.Infof("标记价格分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = o.subscribeOkxMarkPriceMultiple(client, symbols, callBack)
		if err != nil {
			return err
		}
	}
	log.Infof("标记价格订阅结束，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	return nil
}

func (o *OkxMarkPrice) subscribeOkxMarkPriceMultiple(okxWsClient *myokxapi.PublicWsStreamClient, symbols []string, callback func(markPrice *MarkPrice, err error)) error {
	okxSub, err := okxWsClient.SubscribeMarkPriceMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		symbolKey := symbol
		o.WsClientMap.Store(symbolKey, okxWsClient)
		o.SubMap.Store(symbol, okxSub)
		o.CallBackMap.Store(symbol, callback)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case err := <-okxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-okxSub.ResultChan():
				mp := &MarkPrice{
					Exchange:  o.Exchange.String(),
					Symbol:    result.InstId,
					MarkPrice: stringToFloat64(result.MarkPx),
					Timestamp: stringToInt64(result.Ts),
				}
				o.MarkPriceMap.Store(result.InstId, mp)
				if callback != nil {
					callback(mp, nil)
				}
			case <-okxSub.CloseChan():
				log.Info("订阅已关闭: ", okxSub.Args)
				return
			}
		}
	}()
	currentCount := int64(len(symbols))
	count, ok := o.WsClientListMap.Load(okxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		o.WsClientListMap.Store(okxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	wg.Wait()
	return nil
}

func (o *OkxMarkPrice) Close() {
	o.WsClientListMap.Range(func(k *myokxapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	o.MarkPriceMap.Clear()
	o.WsClientListMap.Clear()
	o.WsClientMap.Clear()
	o.SubMap.Clear()
	o.CallBackMap.Clear()
}
