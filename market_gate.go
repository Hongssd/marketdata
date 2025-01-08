package marketdata

import (
	"github.com/Hongssd/mygateapi"
	"github.com/robfig/cron/v3"
)

type GateMarketData struct {
	mygateapi.Client
	ServerTimeDelta int64
	//*GateOrderBook
	*GateKline
	//*GateDepth
	//*GateAggTrade
}

func (gm *GateMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		serverTimeDelta, err := GateGetServerTimeDelta()
		if err != nil {
			log.Error(err)
		}
		gm.ServerTimeDelta = serverTimeDelta
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

// 获取当前服务器时间差
func (gm *GateMarketData) GetServerTimeDelta() int64 {
	return gm.ServerTimeDelta
}
