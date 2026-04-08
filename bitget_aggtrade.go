package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybitgetapi"
)

type BitgetAggTrade struct {
	parent              *BitgetMarketData
	SpotAggTrade        *bitgetAggTradeBase
	UsdtFuturesAggTrade *bitgetAggTradeBase
	CoinFuturesAggTrade *bitgetAggTradeBase
	UsdcFuturesAggTrade *bitgetAggTradeBase
}

type bitgetAggTradeBase struct {
	parent *BitgetAggTrade
	BitgetWsClientBase
	Exchange    Exchange
	AccountType BitgetAccountType
	InstType    mybitgetapi.InstType
	AggTradeMap *MySyncMap[string, *AggTrade]
	WsClientMap *MySyncMap[string, *mybitgetapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsPublicTrade]]
	CallBackMap *MySyncMap[string, func(aggTrade *AggTrade, err error)]
}

func normalizeBitgetAggTradeConfigBase(c *BitgetAggTradeConfigBase) {
	if c.PerSubMaxLen == 0 {
		c.PerSubMaxLen = 50
	}
	if c.PerConnSubNum == 0 {
		c.PerConnSubNum = 100
	}
}

func (bm *BitgetMarketData) newBitgetAggTrade(config BitgetAggTradeConfig) *BitgetAggTrade {
	normalizeBitgetAggTradeConfigBase(&config.SpotConfig)
	normalizeBitgetAggTradeConfigBase(&config.UsdtFuturesConfig)
	normalizeBitgetAggTradeConfigBase(&config.CoinFuturesConfig)
	normalizeBitgetAggTradeConfigBase(&config.UsdcFuturesConfig)
	b := &BitgetAggTrade{parent: bm}
	b.SpotAggTrade = b.newBitgetAggTradeBase(config.SpotConfig, BITGET_SPOT)
	b.SpotAggTrade.parent = b
	b.UsdtFuturesAggTrade = b.newBitgetAggTradeBase(config.UsdtFuturesConfig, BITGET_USDT_FUTURES)
	b.UsdtFuturesAggTrade.parent = b
	b.CoinFuturesAggTrade = b.newBitgetAggTradeBase(config.CoinFuturesConfig, BITGET_COIN_FUTURES)
	b.CoinFuturesAggTrade.parent = b
	b.UsdcFuturesAggTrade = b.newBitgetAggTradeBase(config.UsdcFuturesConfig, BITGET_USDC_FUTURES)
	b.UsdcFuturesAggTrade.parent = b
	return b
}

func (b *BitgetAggTrade) newBitgetAggTradeBase(config BitgetAggTradeConfigBase, accountType BitgetAccountType) *bitgetAggTradeBase {
	instType, _ := BitgetAccountTypeToInstType(accountType)
	return &bitgetAggTradeBase{
		BitgetWsClientBase: BitgetWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybitgetapi.PublicWsStreamClient, *int64]()),
		},
		Exchange:    BITGET,
		AccountType: accountType,
		InstType:    instType,
		AggTradeMap: GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybitgetapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsPublicTrade]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

func (b *BitgetAggTrade) getBaseFromAccountType(accountType BitgetAccountType) (*bitgetAggTradeBase, error) {
	switch accountType {
	case BITGET_SPOT:
		return b.SpotAggTrade, nil
	case BITGET_USDT_FUTURES:
		return b.UsdtFuturesAggTrade, nil
	case BITGET_COIN_FUTURES:
		return b.CoinFuturesAggTrade, nil
	case BITGET_USDC_FUTURES:
		return b.UsdcFuturesAggTrade, nil
	default:
		return nil, ErrorAccountType
	}
}

func (b *BitgetAggTrade) GetLastAggTrade(accountType BitgetAccountType, symbol string) (*AggTrade, error) {
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return nil, err
	}
	t, ok := base.AggTradeMap.Load(symbol)
	if !ok {
		return nil, fmt.Errorf("%s symbol:%s aggTrade not found", accountType, symbol)
	}
	return t, nil
}

func (a *bitgetAggTradeBase) subscribeBitgetAggTradeMultiple(wsClient *mybitgetapi.PublicWsStreamClient, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	sub, err := wsClient.SubscribePublicTradesMultiple(a.InstType, symbols)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		a.WsClientMap.Store(symbol, wsClient)
		a.SubMap.Store(symbol, sub)
		a.CallBackMap.Store(symbol, callback)
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
				sym := result.Arg.Symbol
				for _, row := range result.Data {
					isMarket := true
					if row.Side == "buy" {
						isMarket = false
					}
					now := time.Now().UnixMilli()
					tradeMs := stringToInt64(row.TradeTimestamp)
					targetTs := tradeMs + a.parent.parent.GetServerTimeDelta()
					if targetTs > now {
						targetTs = now
					}
					firstID, _ := strconv.ParseInt(row.TradeId, 10, 64)
					agg := &AggTrade{
						AId:         row.TradeId,
						Exchange:    a.Exchange.String(),
						AccountType: a.AccountType.String(),
						Symbol:      sym,
						Timestamp:   targetTs,
						Price:       stringToFloat64(row.Price),
						Quantity:    stringToFloat64(row.Volume),
						First:       firstID,
						Last:        firstID,
						TradeTime:   tradeMs,
						IsMarket:    isMarket,
					}
					a.AggTradeMap.Store(sym, agg)
					cb, _ := a.CallBackMap.Load(sym)
					if cb != nil {
						cb(agg, nil)
					}
				}
			case <-sub.CloseChan():
				log.Info("Bitget 公共成交订阅已关闭: ", sub.Args)
				return
			}
		}
	}()
	currentCount := int64(len(symbols))
	count, ok := a.WsClientListMap.Load(wsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		a.WsClientListMap.Store(wsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (b *BitgetAggTrade) SubscribeAggTrade(accountType BitgetAccountType, symbol string) error {
	return b.SubscribeAggTradeWithCallBack(accountType, symbol, nil)
}

func (b *BitgetAggTrade) SubscribeAggTrades(accountType BitgetAccountType, symbols []string) error {
	return b.SubscribeAggTradesWithCallBack(accountType, symbols, nil)
}

func (b *BitgetAggTrade) SubscribeAggTradeWithCallBack(accountType BitgetAccountType, symbol string, callback func(aggTrade *AggTrade, err error)) error {
	return b.SubscribeAggTradesWithCallBack(accountType, []string{symbol}, callback)
}

func (b *BitgetAggTrade) SubscribeAggTradesWithCallBack(accountType BitgetAccountType, symbols []string, callback func(aggTrade *AggTrade, err error)) error {
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return err
	}
	log.Infof("开始订阅 Bitget 公共成交，accountType:%s，交易对数:%d", accountType, len(symbols))
	LEN := base.perSubMaxLen
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
			if err = base.subscribeBitgetAggTradeMultiple(client, batch, callback); err != nil {
				return err
			}
			currentCount, ok := base.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Bitget 成交分批订阅成功，本批:%v，链接订阅数:%d，等待1秒...", batch, *currentCount)
			time.Sleep(time.Second)
		}
	} else {
		client, err := base.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		if err = base.subscribeBitgetAggTradeMultiple(client, symbols, callback); err != nil {
			return err
		}
	}
	log.Infof("Bitget 公共成交订阅结束，accountType:%s", accountType)
	return nil
}

func (a *bitgetAggTradeBase) Close() {
	a.BitgetWsClientBase.close()
	a.AggTradeMap.Clear()
	a.WsClientMap.Clear()
	a.SubMap.Clear()
	a.CallBackMap.Clear()
}

func (b *BitgetAggTrade) Close() {
	b.SpotAggTrade.Close()
	b.UsdtFuturesAggTrade.Close()
	b.CoinFuturesAggTrade.Close()
	b.UsdcFuturesAggTrade.Close()
}
