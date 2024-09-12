package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/myokxapi"
	"sync/atomic"
	"time"
)

type OkxOption struct {
	parent                      *OkxMarketData
	perConnSubNum               int64
	perSubMaxLen                int
	PerOptionMarkPriceSubNum    int64
	PerOptionMarkPriceSubMaxLen int
	Exchange                    Exchange
	OptionMap                   *MySyncMap[string, *OptionTicker]
	WsClientListMap             *MySyncMap[*myokxapi.PublicWsStreamClient, *int64]
	WsClientMap                 *MySyncMap[string, *myokxapi.PublicWsStreamClient]
	SubMap                      *MySyncMap[string, *myokxapi.Subscription[myokxapi.WsOptSummary]]
	CallBackMap                 *MySyncMap[string, func(ot *OptionTicker, err error)]
}

func (om *OkxMarketData) newOkxOption(config OkxOptionConfig) *OkxOption {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 1000
	}
	if config.PerOptionMarkPriceSubNum == 0 {
		config.PerOptionMarkPriceSubNum = 100
	}
	if config.PerOptionMarkPriceSubMaxLen == 0 {
		config.PerOptionMarkPriceSubMaxLen = 1000
	}
	o := &OkxOption{
		parent:          om,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        "OKX",
		OptionMap:       GetPointer(NewMySyncMap[string, *OptionTicker]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myokxapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myokxapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsOptSummary]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(ot *OptionTicker, err error)]()),
	}

	return o
}

func (o *OkxOption) GetLastOption(symbol string) (*OptionTicker, error) {
	ot, ok := o.OptionMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s option not found", symbol)
		return nil, err
	}
	return ot, nil
}

func (o *OkxOption) GetCurrentOrNewWsClient() (*myokxapi.PublicWsStreamClient, error) {
	return o.parent.GetPublicCurrentOrNewWsClient(o.perConnSubNum, o.WsClientListMap)
}

func (o *OkxOption) SubscribeOption(symbol string) error {
	return o.SubscribeOptionWithCallBack(symbol, nil)
}

func (o *OkxOption) SubscribeOptions(symbols []string) error {
	return o.SubscribeOptionsWithCallBack(symbols, nil)
}

func (o *OkxOption) SubscribeOptionWithCallBack(symbol string, callBack func(ot *OptionTicker, err error)) error {
	return o.SubscribeOptionsWithCallBack([]string{symbol}, callBack)
}

func (o *OkxOption) SubscribeOptionsWithCallBack(symbols []string, callback func(ot *OptionTicker, err error)) error {
	log.Infof("开始订阅Option，交易对数量:%d，总订阅数:%d", len(symbols), len(symbols))

	LEN := o.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbol := symbols[i:end]
			client, err := o.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = o.subscribeOkxOptionMultiple(client, tempSymbol, callback)
			if err != nil {
				return err
			}
			currentCount, ok := o.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Option订阅结束，此次订阅交易对:%v，总订阅数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbol, len(tempSymbol), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := o.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = o.subscribeOkxOptionMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}
	log.Infof("Option订阅结束，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	return nil
}

func (o *OkxOption) GetOptionInstIds(ulys []string) ([]string, error) {
	var instIds []string
	for _, uly := range ulys {
		res, err := okx.NewRestClient(o.parent.APIKey, o.parent.SecretKey, o.parent.Passphrase).PublicRestClient().NewPublicRestPublicInstruments().
			InstType("OPTION").
			Uly(uly).
			Do()
		if err != nil {
			log.Error(err)
			return nil, err
		}
		for _, instId := range res.Data {
			instIds = append(instIds, instId.InstId)
		}
	}

	return instIds, nil
}

func (o *OkxOption) subscribeOkxOptionMultiple(okxWsClient *myokxapi.PublicWsStreamClient, symbols []string, callback func(ot *OptionTicker, err error)) error {
	//wg := sync.WaitGroup{}

	osSub, err := okxWsClient.SubscribeOptSummaryMultiple(symbols) // 期权定价ws opt-summary 频道订阅
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		symbolKey := symbol
		o.WsClientMap.Store(symbolKey, okxWsClient)
		o.SubMap.Store(symbolKey, osSub)
		o.CallBackMap.Store(symbolKey, callback)
	}

	//wg.Add(1)
	go func() {
		//defer wg.Done()
		for {
			select {
			case err := <-osSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-osSub.ResultChan():
				ot := &OptionTicker{
					Timestamp:      stringToInt64(result.Ts),
					Exchange:       o.Exchange.String(),
					InstrumentName: result.InstId,
					Greeks: OptionTickerGreeks{
						Vega:  stringToFloat64(result.VegaBS),
						Theta: stringToFloat64(result.ThetaBS),
						Gamma: stringToFloat64(result.GammaBS),
						Delta: stringToFloat64(result.DeltaBS),
					},
					MarkIv:     stringToFloat64(result.MarkVol),
					IndexPrice: stringToFloat64(result.FwdPx),
					AskIv:      stringToFloat64(result.AskVol),
					BidIv:      stringToFloat64(result.BidVol),
				}
				o.OptionMap.Store(result.InstId, ot)
				if callback != nil {
					callback(ot, nil)
				}
			case <-osSub.CloseChan():
				log.Info("订阅已关闭: ", osSub.Args)
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

	//wg.Wait()
	return nil
}

func (o *OkxOption) Close() {
	o.WsClientListMap.Range(func(k *myokxapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	o.OptionMap.Clear()
	o.WsClientListMap.Clear()
	o.WsClientMap.Clear()
	o.SubMap.Clear()
	o.CallBackMap.Clear()
}
