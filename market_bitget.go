package marketdata

import (
	"math"

	"github.com/Hongssd/mybitgetapi"
	"github.com/robfig/cron/v3"
)

type BitgetMarketData struct {
	mybitgetapi.Client
	serverTimeDelta      int64
	serverTimeDeltaTimes int64
	serverTimeDeltaSum   int64
	serverTimeDeltaCron  *cron.Cron
	*BitgetTickers
	*BitgetDepth
	*BitgetKline
	*BitgetAggTrade
	*BitgetOrderBook
}

var BitgetServerTimeDeltaActive bool = true

func SetBitgetServerTimeDeltaActive(active bool) {
	BitgetServerTimeDeltaActive = active
}

func NewBitgetMarketDataDefault() (*BitgetMarketData, error) {
	return NewBitgetMarketData("", "", "")
}

func NewBitgetMarketData(apiKey, apiSecret, passphrase string) (*BitgetMarketData, error) {
	marketData := &BitgetMarketData{
		Client: mybitgetapi.Client{
			ApiKey:     apiKey,
			ApiSecret:  apiSecret,
			Passphrase: passphrase,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}

func (bm *BitgetMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		if !BitgetServerTimeDeltaActive {
			bm.serverTimeDelta = 0
			bm.serverTimeDeltaTimes = 0
			bm.serverTimeDeltaSum = 0
			return
		}
		serverTimeDelta, err := BitgetGetServerTimeDelta()
		if err != nil {
			log.Error(err)
			return
		}
		if bm.serverTimeDeltaTimes > 10 && bm.serverTimeDelta != 0 {
			if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(3*bm.serverTimeDelta)) {
				return
			}
		}
		bm.serverTimeDeltaTimes++
		bm.serverTimeDeltaSum += serverTimeDelta
		bm.serverTimeDelta = bm.serverTimeDeltaSum / bm.serverTimeDeltaTimes
	}
	refresh()

	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	bm.serverTimeDeltaCron = c
	return nil
}

func (bm *BitgetMarketData) InitBitgetTickers(config BitgetTickersConfig) error {
	b := bm.newBitgetTickers(config)
	bm.BitgetTickers = b
	return nil
}

func (bm *BitgetMarketData) InitBitgetDepth(config BitgetDepthConfig) error {
	d := bm.newBitgetDepth(config)
	bm.BitgetDepth = d
	return nil
}

func (bm *BitgetMarketData) InitBitgetKline(config BitgetKlineConfig) error {
	k := bm.newBitgetKline(config)
	bm.BitgetKline = k
	return nil
}

func (bm *BitgetMarketData) InitBitgetAggTrade(config BitgetAggTradeConfig) error {
	a := bm.newBitgetAggTrade(config)
	bm.BitgetAggTrade = a
	return nil
}

func (bm *BitgetMarketData) InitBitgetOrderBook(config BitgetOrderBookConfig) error {
	o := bm.newBitgetOrderBook(config)
	bm.BitgetOrderBook = o
	return nil
}

func (bm *BitgetMarketData) GetServerTimeDelta() int64 {
	return bm.serverTimeDelta
}

func (bm *BitgetMarketData) Close() {
	if bm.serverTimeDeltaCron != nil {
		bm.serverTimeDeltaCron.Stop()
	}
}
