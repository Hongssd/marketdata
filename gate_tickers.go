package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mygateapi"
	"github.com/shopspring/decimal"
)

type GateTickers struct {
	parent          *GateMarketData
	SpotTickers     *gateTickersBase
	FuturesTickers  *gateTickersBase
	DeliveryTickers *gateTickersBase
}

type gateTickersBase struct {
	parent *GateTickers
	GateWsClientBase
	Exchange    Exchange
	AccountType GateAccountType
	TickerMap   *MySyncMap[string, *Ticker]                                                                          //symbol->last ticker
	WsClientMap *MySyncMap[string, *mygateapi.WsStreamClient]                                                        //symbol->ws client
	SubMap      *MySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsTicker]]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(ticker *Ticker, err error)]                                                  //symbol->callback
}

func (b *GateTickers) getBaseMapFromAccountType(accountType GateAccountType) (*gateTickersBase, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotTickers, nil
	case GATE_FUTURES:
		return b.FuturesTickers, nil
	case GATE_DELIVERY:
		return b.DeliveryTickers, nil
	}
	return nil, ErrorAccountType
}

func (b *GateTickers) newGateTickersBase(config GateTickersConfigBase) *gateTickersBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &gateTickersBase{
		Exchange: GATE,
		GateWsClientBase: GateWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mygateapi.WsStreamClient, *int64]()),
		},
		TickerMap:   GetPointer(NewMySyncMap[string, *Ticker]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mygateapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsTicker]]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(ticker *Ticker, err error)]()),
	}
}

// 封装好的获取ticker方法
func (b *GateTickers) GetLastTicker(GateAccountType GateAccountType, symbol string) (*Ticker, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(GateAccountType)
	if err != nil {
		return nil, err
	}
	ticker, ok := bmap.TickerMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s ticker not found", GateAccountType, symbol)
		return nil, err
	}
	return ticker, nil
}

func (b *GateTickers) GetCurrentOrNewWsClient(accountType GateAccountType) (*mygateapi.WsStreamClient, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotTickers.GetCurrentOrNewWsClient(accountType)
	case GATE_FUTURES:
		return b.FuturesTickers.GetCurrentOrNewWsClient(accountType)
	case GATE_DELIVERY:
		return b.DeliveryTickers.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (b *gateTickersBase) subscribeGateTicker(gateWsClient *mygateapi.WsStreamClient, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.subscribeGateTickersMultiple(gateWsClient, []string{symbol}, callback)
}

// 订阅Gate ticker底层执行
func (b *gateTickersBase) subscribeGateTickersMultiple(gateWsClient *mygateapi.WsStreamClient, symbols []string, callback func(ticker *Ticker, err error)) error {

	gateSub, err := gateWsClient.SubscribeTickerMultiple(symbols)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, gateWsClient)
		b.SubMap.Store(symbolKey, gateSub)
		b.CallBackMap.Store(symbolKey, callback)
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
				symbolKey := result.Symbol
				if symbolKey == "" {
					symbolKey = result.Contract
				}

				now := time.Now().UnixMilli()
				targetTs := r.TimeMs + b.parent.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}

				// 计算24小时价格变化百分比
				var price24hPercent float64
				lastPrice, _ := decimal.NewFromString(result.Last)
				changePercent, _ := decimal.NewFromString(result.ChangePercentage)
				if !changePercent.IsZero() {
					price24hPercent = changePercent.InexactFloat64()
				}

				// 计算24小时前价格
				var prevPrice24h float64
				if !lastPrice.IsZero() && !changePercent.IsZero() {
					// prevPrice = lastPrice / (1 + changePercent/100)
					percentDecimal := changePercent.Div(decimal.NewFromInt(100))
					prevPrice24h = lastPrice.Div(decimal.NewFromInt(1).Add(percentDecimal)).InexactFloat64()
				}

				//保存至Ticker
				ticker := &Ticker{
					Timestamp:       targetTs,
					Exchange:        b.Exchange.String(),
					AccountType:     b.AccountType.String(),
					Symbol:          symbolKey,
					LastPrice:       stringToFloat64(result.Last),
					Price24hPercent: price24hPercent,
					PrevPrice24h:    prevPrice24h,
					HighPrice24h:    stringToFloat64(result.High24h),
					LowPrice24h:     stringToFloat64(result.Low24h),
					Volume24h:       stringToFloat64(result.BaseVolume),
					Turnover24h:     stringToFloat64(result.QuoteVolume),
				}
				b.TickerMap.Store(symbolKey, ticker)
				if callback != nil {
					callback(ticker, nil)
				}
			case <-gateSub.CloseChan():
				log.Info("订阅已关闭: ", gateSub.SubKeys)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(gateWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(gateWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅Gate ticker
func (b *gateTickersBase) UnSubscribeGateTicker(symbol string) error {
	symbolKey := symbol
	gateSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return gateSub.Unsubscribe()
}

// 订阅ticker
func (b *GateTickers) SubscribeTicker(accountType GateAccountType, symbol string) error {
	return b.SubscribeTickerWithCallBack(accountType, symbol, nil)
}

// 批量订阅ticker
func (b *GateTickers) SubscribeTickers(accountType GateAccountType, symbols []string) error {
	return b.SubscribeTickersWithCallBack(accountType, symbols, nil)
}

// 订阅ticker并带上回调
func (b *GateTickers) SubscribeTickerWithCallBack(accountType GateAccountType, symbol string, callback func(ticker *Ticker, err error)) error {
	return b.SubscribeTickersWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅ticker并带上回调
func (b *GateTickers) SubscribeTickersWithCallBack(accountType GateAccountType, symbols []string, callback func(ticker *Ticker, err error)) error {
	log.Infof("开始订阅ticker%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentGateTickersBase *gateTickersBase

	switch accountType {
	case GATE_SPOT:
		currentGateTickersBase = b.SpotTickers
	case GATE_FUTURES:
		currentGateTickersBase = b.FuturesTickers
	case GATE_DELIVERY:
		currentGateTickersBase = b.DeliveryTickers
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentGateTickersBase.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := b.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentGateTickersBase.subscribeGateTickersMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentGateTickersBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("ticker%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentGateTickersBase.subscribeGateTickersMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("ticker订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *gateTickersBase) Close() {
	b.GateWsClientBase.close()

	b.TickerMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()
}

func (b *GateTickers) Close() {
	b.SpotTickers.Close()
	b.FuturesTickers.Close()
	b.DeliveryTickers.Close()
}
