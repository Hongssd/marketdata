package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mysunxapi"
)

type SunxAggTrade struct {
	parent *SunxMarketData

	SwapAggTrade *sunxAggTradeBase
}

type sunxAggTradeBase struct {
	parent *SunxAggTrade
	SunxWsClientBase
	Exchange    Exchange
	AccountType SunxAccountType
	AggTradeMap *MySyncMap[string, *AggTrade]
	WsClientMap *MySyncMap[string, *mysunxapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketCommonReq, mysunxapi.WsTradeDetail]]
	CallBackMap *MySyncMap[string, func(aggTrade *AggTrade, err error)]
}

func (a *SunxAggTrade) getBaseMapFromAccountType(accountType SunxAccountType) (*sunxAggTradeBase, error) {
	switch accountType {
	case SUNX_SWAP:
		return a.SwapAggTrade, nil
	}
	return nil, ErrorAccountType
}

func (a *SunxAggTrade) newSunxAggTradeBase(config SunxAggTradeConfigBase) *sunxAggTradeBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	return &sunxAggTradeBase{
		Exchange: SUNX,
		SunxWsClientBase: SunxWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mysunxapi.PublicWsStreamClient, *int64]()),
		},
		AggTradeMap: GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mysunxapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketCommonReq, mysunxapi.WsTradeDetail]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

// 封装好的获取交易流方法
func (a *SunxAggTrade) GetLastAggTrade(accountType SunxAccountType, symbol string) (*AggTrade, error) {
	symbolKey := symbol

	bmap, err := a.getBaseMapFromAccountType(accountType)
	if err != nil {
		return nil, err
	}
	aggTrade, ok := bmap.AggTradeMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s aggTrade not found", accountType, symbol)
		return nil, err
	}
	return aggTrade, nil
}

func (a *SunxAggTrade) GetCurrentOrNewWsClient(accountType SunxAccountType) (*mysunxapi.PublicWsStreamClient, error) {
	switch accountType {
	case SUNX_SWAP:
		return a.SwapAggTrade.GetCurrentOrNewWsClient(accountType)
	}
	return nil, ErrorAccountType
}

// 订阅SunX有限档交易流底层执行
func (a *sunxAggTradeBase) subscribeSunxAggTradeMultiple(sunxWsClient *mysunxapi.PublicWsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	sunxSub, err := sunxWsClient.SubscribeMarketTradeDetail(symbols, true)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		a.WsClientMap.Store(symbolKey, sunxWsClient)
		a.SubMap.Store(symbolKey, sunxSub)
		a.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-sunxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-sunxSub.ResultChan():
				result := r.Tick.Data
				split := strings.Split(r.Ch, ".")
				if len(split) == 0 {
					log.Error("subscribe err, r.ch is empty")
					continue
				}
				symbolKey := split[1]

				now := time.Now().UnixMilli()
				for _, trade := range result {
					targetTs := trade.Ts + a.parent.parent.GetServerTimeDelta()
					if targetTs > now {
						targetTs = now
					}
					aggTrade := &AggTrade{
						AId:         strconv.FormatInt(trade.Id, 10),
						Exchange:    a.Exchange.String(),
						AccountType: SUNX_SWAP,
						Symbol:      symbolKey,
						Timestamp:   targetTs,
						Price:       trade.Price,
						Quantity:    trade.Quantity,
						First:       trade.Id,
						Last:        trade.Id,
						TradeTime:   targetTs,
						IsMarket:    trade.Direction == "buy",
					}
					a.AggTradeMap.Store(symbolKey, aggTrade)
					if callback != nil {
						callback(aggTrade, nil)
					}
				}
			case <-sunxSub.CloseChan():
				log.Info("订阅已关闭: ", sunxSub.SubReqs)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := a.WsClientListMap.Load(sunxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		a.WsClientListMap.Store(sunxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅交易流
func (a *sunxAggTradeBase) UnSubscribeGateAggTrade(symbol string) error {
	symbolKey := symbol
	sunxSub, ok := a.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return sunxSub.Unsubscribe()
}

// 订阅交易流
func (a *SunxAggTrade) SubscribeAggTrade(accountType SunxAccountType, symbol string) error {
	return a.SubscribeAggTradeWithCallBack(accountType, symbol, nil)
}

// 批量订阅交易流
func (a *SunxAggTrade) SubscribeAggTrades(accountType SunxAccountType, symbols []string) error {
	return a.SubscribeAggTradesWithCallBack(accountType, symbols, nil)
}

// 订阅交易流并带上回调
func (a *SunxAggTrade) SubscribeAggTradeWithCallBack(accountType SunxAccountType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return a.SubscribeAggTradesWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅交易流并带上回调
func (a *SunxAggTrade) SubscribeAggTradesWithCallBack(accountType SunxAccountType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	log.Infof("开始订阅归集交易流%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentSunxAggTradeBase *sunxAggTradeBase
	switch accountType {
	case SUNX_SWAP:
		currentSunxAggTradeBase = a.SwapAggTrade
	default:
		return ErrorAccountType
	}

	LEN := currentSunxAggTradeBase.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := a.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentSunxAggTradeBase.subscribeSunxAggTradeMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := currentSunxAggTradeBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("归集交易流%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := a.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentSunxAggTradeBase.subscribeSunxAggTradeMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}
	log.Infof("归集交易流订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))
	return nil
}

func (a *sunxAggTradeBase) Close() {
	a.SunxWsClientBase.close()
	a.AggTradeMap.Clear()
	a.WsClientMap.Clear()
	a.SubMap.Clear()
	a.CallBackMap.Clear()
}

func (a *SunxAggTrade) Close() {
	a.SwapAggTrade.Close()
}
