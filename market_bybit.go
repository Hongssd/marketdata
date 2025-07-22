package marketdata

import (
	"math"

	"github.com/Hongssd/mybybitapi"
	"github.com/robfig/cron/v3"
)

type BybitMarketData struct {
	mybybitapi.Client
	ServerTimeDelta      int64
	ServerTimeDeltaTimes int64
	ServerTimeDeltaSum   int64
	serverTimeDeltaCron  *cron.Cron
	*BybitKline
	*BybitOrderBook
	*BybitAggTrade
	*BybitTickers
}

func (bm *BybitMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		serverTimeDelta, err := BybitGetServerTimeDelta()
		if err != nil {
			log.Error(err)
		}
		//丢弃高波动均值影响
		if bm.ServerTimeDeltaTimes > 10 && bm.ServerTimeDelta != 0 {
			if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(3*bm.ServerTimeDelta)) {
				return
			}
		}
		bm.ServerTimeDeltaTimes++
		bm.ServerTimeDeltaSum += serverTimeDelta
		bm.ServerTimeDelta = bm.ServerTimeDeltaSum / bm.ServerTimeDeltaTimes
	}
	refresh()

	//每隔15秒更新一次服务器时间
	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	bm.serverTimeDeltaCron = c
	return nil
}

func NewBybitMarketDataDefault() (*BybitMarketData, error) {
	return NewBybitMarketData("", "")
}
func NewBybitMarketData(apiKey, apiSecrey string) (*BybitMarketData, error) {
	marketData := &BybitMarketData{
		Client: mybybitapi.Client{
			APIKey:    apiKey,
			SecretKey: apiSecrey,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}

func (bm *BybitMarketData) InitBybitKline(config BybitKlineConfig) error {
	b := &BybitKline{}
	b.SpotKline = b.newBybitKlineBase(config.SpotConfig)
	b.SpotKline.AccountType = BYBIT_SPOT
	b.SpotKline.parent = b
	b.LinearKline = b.newBybitKlineBase(config.LinearConfig)
	b.LinearKline.AccountType = BYBIT_LINEAR
	b.LinearKline.parent = b
	b.InverseKline = b.newBybitKlineBase(config.InverseConfig)
	b.InverseKline.AccountType = BYBIT_INVERSE
	b.InverseKline.parent = b
	bm.BybitKline = b
	b.parent = bm
	return nil
}
func (bm *BybitMarketData) InitBybitOrderBook(config BybitOrderBookConfig) error {
	b := &BybitOrderBook{}
	b.SpotOrderBook = b.newBybitOrderBookBase(config.SpotConfig)
	b.SpotOrderBook.AccountType = BYBIT_SPOT
	b.SpotOrderBook.parent = b
	b.LinearOrderBook = b.newBybitOrderBookBase(config.LinearConfig)
	b.LinearOrderBook.AccountType = BYBIT_LINEAR
	b.LinearOrderBook.parent = b
	b.InverseOrderBook = b.newBybitOrderBookBase(config.InverseConfig)
	b.InverseOrderBook.AccountType = BYBIT_INVERSE
	b.InverseOrderBook.parent = b
	b.init()
	bm.BybitOrderBook = b
	b.parent = bm
	return nil
}

func (bm *BybitMarketData) InitBybitAggTrade(config BybitAggTradeConfig) error {
	b := &BybitAggTrade{}
	b.SpotAggTrade = b.newBybitAggTradeBase(config.SpotConfig)
	b.SpotAggTrade.AccountType = BYBIT_SPOT
	b.SpotAggTrade.parent = b
	b.LinearAggTrade = b.newBybitAggTradeBase(config.LinearConfig)
	b.LinearAggTrade.AccountType = BYBIT_LINEAR
	b.LinearAggTrade.parent = b
	b.InverseAggTrade = b.newBybitAggTradeBase(config.InverseConfig)
	b.InverseAggTrade.AccountType = BYBIT_INVERSE
	b.InverseAggTrade.parent = b
	bm.BybitAggTrade = b
	b.parent = bm
	return nil
}

func (bm *BybitMarketData) InitBybitTickers(config BybitTickersConfig) error {
	b := &BybitTickers{}
	b.SpotTickers = b.newBybitTickersBase(config.SpotConfig)
	b.SpotTickers.AccountType = BYBIT_SPOT
	b.SpotTickers.parent = b
	b.LinearTickers = b.newBybitTickersBase(config.LinearConfig)
	b.LinearTickers.AccountType = BYBIT_LINEAR
	b.LinearTickers.parent = b
	b.InverseTickers = b.newBybitTickersBase(config.InverseConfig)
	b.InverseTickers.AccountType = BYBIT_INVERSE
	b.InverseTickers.parent = b
	b.OptionTickers = b.newBybitTickersBase(config.OptionConfig)
	b.OptionTickers.AccountType = BYBIT_OPTION
	b.OptionTickers.parent = b
	bm.BybitTickers = b
	b.parent = bm
	return nil
}

// 获取当前服务器时间差
func (bm *BybitMarketData) GetServerTimeDelta() int64 {
	return bm.ServerTimeDelta
}

func (bm *BybitMarketData) Close() {
	if bm.serverTimeDeltaCron != nil {
		bm.serverTimeDeltaCron.Stop()
	}
}
