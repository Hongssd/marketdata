package marketdata

import (
	"math"
	"sync"

	"github.com/Hongssd/myasterapi"
	"github.com/robfig/cron/v3"
)

type AsterMarketData struct {
	myasterapi.Client
	spotServerTimeDelta   int64
	futureServerTimeDelta int64

	//增加平均值机制
	spotServerTimeDeltaTimes   int64
	futureServerTimeDeltaTimes int64

	spotServerTimeDeltaSum   int64
	futureServerTimeDeltaSum int64

	serverTimeDeltaCron *cron.Cron
	*AsterOrderBook
	*AsterKline
	*AsterDepth
	*AsterAggTrade
	*AsterTickers
}

var AsterServerTimeDeltaActive bool = true

func SetAsterServerTimeDeltaActive(active bool) {
	AsterServerTimeDeltaActive = active
}

func (bm *AsterMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if !AsterServerTimeDeltaActive {
				bm.spotServerTimeDelta = 0
				bm.spotServerTimeDeltaTimes = 0
				bm.spotServerTimeDeltaSum = 0
				return
			}
			serverTimeDelta, err := AsterGetServerTimeDelta(ASTER_SPOT)
			if err != nil {
				log.Error(err)
			}
			//丢弃高波动均值影响
			if bm.spotServerTimeDeltaTimes > 10 && bm.spotServerTimeDelta != 0 {
				if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(2*bm.spotServerTimeDelta)) {
					return
				}
			}
			bm.spotServerTimeDeltaTimes++
			bm.spotServerTimeDeltaSum += serverTimeDelta
			bm.spotServerTimeDelta = bm.spotServerTimeDeltaSum / bm.spotServerTimeDeltaTimes
		}()
		go func() {
			defer wg.Done()
			if !AsterServerTimeDeltaActive {
				bm.futureServerTimeDelta = 0
				bm.futureServerTimeDeltaTimes = 0
				bm.futureServerTimeDeltaSum = 0
				return
			}
			serverTimeDelta, err := AsterGetServerTimeDelta(ASTER_FUTURE)
			if err != nil {
				log.Error(err)
			}
			//丢弃高波动均值影响
			if bm.futureServerTimeDeltaTimes > 10 && bm.futureServerTimeDelta != 0 {
				if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(2*bm.futureServerTimeDelta)) {
					return
				}
			}
			bm.futureServerTimeDeltaTimes++
			bm.futureServerTimeDeltaSum += serverTimeDelta
			bm.futureServerTimeDelta = bm.futureServerTimeDeltaSum / bm.futureServerTimeDeltaTimes
		}()
		wg.Wait()
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

func NewAsterMarketDataDefault() (*AsterMarketData, error) {
	return NewAsterMarketData("", "")
}
func NewAsterMarketData(apiKey, apiSecrey string) (*AsterMarketData, error) {
	marketData := &AsterMarketData{
		Client: myasterapi.Client{
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
func (bm *AsterMarketData) InitAsterOrderBook(config AsterOrderBookConfig) error {
	b := &AsterOrderBook{}
	b.SpotOrderBook = b.newAsterOrderBookBase(config.SpotConfig)
	b.SpotOrderBook.AccountType = ASTER_SPOT
	b.SpotOrderBook.parent = b
	b.FutureOrderBook = b.newAsterOrderBookBase(config.FutureConfig)
	b.FutureOrderBook.AccountType = ASTER_FUTURE
	b.FutureOrderBook.parent = b
	b.init()
	bm.AsterOrderBook = b
	b.parent = bm
	log.Infof("初始化AsterOrderBook成功，账户类型:%s", ASTER_SPOT)
	return nil
}

func (bm *AsterMarketData) InitAsterKline(config AsterKlineConfig) error {
	b := &AsterKline{}
	b.SpotKline = b.newAsterKlineBase(config.SpotConfig)
	b.SpotKline.AccountType = ASTER_SPOT
	b.SpotKline.parent = b
	b.FutureKline = b.newAsterKlineBase(config.FutureConfig)
	b.FutureKline.AccountType = ASTER_FUTURE
	b.FutureKline.parent = b
	bm.AsterKline = b
	b.parent = bm
	return nil
}

func (bm *AsterMarketData) InitAsterDepth(config AsterDepthConfig) error {
	b := &AsterDepth{}
	b.SpotDepth = b.newAsterDepthBase(config.SpotConfig)
	b.SpotDepth.AccountType = ASTER_SPOT
	b.SpotDepth.parent = b
	b.FutureDepth = b.newAsterDepthBase(config.FutureConfig)
	b.FutureDepth.AccountType = ASTER_FUTURE
	b.FutureDepth.parent = b
	bm.AsterDepth = b
	b.parent = bm
	return nil
}

func (bm *AsterMarketData) InitAsterAggTrade(config AsterAggTradeConfig) error {
	b := &AsterAggTrade{}
	b.SpotAggTrade = b.newAsterAggTradeBase(config.SpotConfig)
	b.SpotAggTrade.AccountType = ASTER_SPOT
	b.SpotAggTrade.parent = b
	b.FutureAggTrade = b.newAsterAggTradeBase(config.FutureConfig)
	b.FutureAggTrade.AccountType = ASTER_FUTURE
	b.FutureAggTrade.parent = b
	bm.AsterAggTrade = b
	b.parent = bm
	return nil
}

func (bm *AsterMarketData) InitAsterTickers(config AsterTickersConfig) error {
	b := &AsterTickers{}
	b.SpotTickers = b.newAsterTickersBase(config.SpotConfig)
	b.SpotTickers.AccountType = ASTER_SPOT
	b.SpotTickers.parent = b
	b.FutureTickers = b.newAsterTickersBase(config.FutureConfig)
	b.FutureTickers.AccountType = ASTER_FUTURE
	b.FutureTickers.parent = b
	bm.AsterTickers = b
	b.parent = bm
	return nil
}

// 获取当前服务器时间差
func (bm *AsterMarketData) GetServerTimeDelta(accountType AsterAccountType) int64 {
	switch accountType {
	case ASTER_SPOT:
		return bm.spotServerTimeDelta
	case ASTER_FUTURE:
		return bm.futureServerTimeDelta
	default:
		return 0
	}
}

func (bm *AsterMarketData) Close() {
	if bm.serverTimeDeltaCron != nil {
		bm.serverTimeDeltaCron.Stop()
	}
}
