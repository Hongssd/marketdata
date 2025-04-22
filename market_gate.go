package marketdata

import (
	"math"

	"github.com/Hongssd/mygateapi"
	"github.com/robfig/cron/v3"
)

type GateMarketData struct {
	mygateapi.Client
	ServerTimeDelta      int64
	ServerTimeDeltaTimes int64
	ServerTimeDeltaSum   int64
	*GateOrderBook
	*GateKline
	*GateDepth
	*GateAggTrade
}

func (gm *GateMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		serverTimeDelta, err := GateGetServerTimeDelta()
		if err != nil {
			log.Error(err)
		}
		//丢弃高波动均值影响
		if gm.ServerTimeDeltaTimes > 10 && gm.ServerTimeDelta != 0 {
			if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(3*gm.ServerTimeDelta)) {
				return
			}
		}
		gm.ServerTimeDeltaTimes++
		gm.ServerTimeDeltaSum += serverTimeDelta
		gm.ServerTimeDelta = gm.ServerTimeDeltaSum / gm.ServerTimeDeltaTimes
	}
	refresh()

	//每隔15秒更新一次服务器时间
	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	return nil
}

func NewGateMarketDataDefault() (*GateMarketData, error) {
	return NewGateMarketData("", "")
}
func NewGateMarketData(apiKey, apiSecrey string) (*GateMarketData, error) {
	marketData := &GateMarketData{
		Client: mygateapi.Client{
			ApiKey:    apiKey,
			ApiSecret: apiSecrey,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}

func (bm *GateMarketData) InitGateOrderBook(config GateOrderBookConfig) error {
	b := &GateOrderBook{}
	b.SpotOrderBook = b.newGateOrderBookBase(config.SpotConfig)
	b.SpotOrderBook.AccountType = GATE_SPOT
	b.SpotOrderBook.parent = b
	b.FuturesOrderBook = b.newGateOrderBookBase(config.FuturesConfig)
	b.FuturesOrderBook.AccountType = GATE_FUTURES
	b.FuturesOrderBook.parent = b
	b.DeliveryOrderBook = b.newGateOrderBookBase(config.DeliveryConfig)
	b.DeliveryOrderBook.AccountType = GATE_DELIVERY
	b.DeliveryOrderBook.parent = b
	b.init()
	bm.GateOrderBook = b
	b.parent = bm
	return nil
}
func (gm *GateMarketData) InitGateKline(config GateKlineConfig) error {
	g := &GateKline{}
	g.SpotKline = g.newGateKlineBase(config.SpotConfig)
	g.SpotKline.AccountType = GATE_SPOT
	g.SpotKline.parent = g
	g.FuturesKline = g.newGateKlineBase(config.FuturesConfig)
	g.FuturesKline.AccountType = GATE_FUTURES
	g.FuturesKline.parent = g
	g.DeliveryKline = g.newGateKlineBase(config.DeliveryConfig)
	g.DeliveryKline.AccountType = GATE_DELIVERY
	g.DeliveryKline.parent = g
	gm.GateKline = g
	g.parent = gm
	return nil
}

func (bm *GateMarketData) InitGateDepth(config GateDepthConfig) error {
	b := &GateDepth{}
	b.SpotDepth = b.newGateDepthBase(config.SpotConfig)
	b.SpotDepth.AccountType = GATE_SPOT
	b.SpotDepth.parent = b
	b.FuturesDepth = b.newGateDepthBase(config.FuturesConfig)
	b.FuturesDepth.AccountType = GATE_FUTURES
	b.FuturesDepth.parent = b
	b.DeliveryDepth = b.newGateDepthBase(config.DeliveryConfig)
	b.DeliveryDepth.AccountType = GATE_DELIVERY
	b.DeliveryDepth.parent = b
	bm.GateDepth = b
	b.parent = bm
	return nil
}

func (bm *GateMarketData) InitGateAggTrade(config GateAggTradeConfig) error {
	b := &GateAggTrade{}
	b.SpotAggTrade = b.newGateAggTradeBase(config.SpotConfig)
	b.SpotAggTrade.AccountType = GATE_SPOT
	b.SpotAggTrade.parent = b
	b.FuturesAggTrade = b.newGateAggTradeBase(config.FuturesConfig)
	b.FuturesAggTrade.AccountType = GATE_FUTURES
	b.FuturesAggTrade.parent = b
	b.DeliveryAggTrade = b.newGateAggTradeBase(config.DeliveryConfig)
	b.DeliveryAggTrade.AccountType = GATE_DELIVERY
	b.DeliveryAggTrade.parent = b
	bm.GateAggTrade = b
	b.parent = bm
	return nil
}

// 获取当前服务器时间差
func (gm *GateMarketData) GetServerTimeDelta() int64 {
	return gm.ServerTimeDelta
}
