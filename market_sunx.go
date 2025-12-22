package marketdata

import (
	"math"

	"github.com/Hongssd/mysunxapi"
	"github.com/robfig/cron/v3"
)

type SunxMarketData struct {
	mysunxapi.Client
	ServerTimeDelta      int64
	ServerTimeDeltaTimes int64
	ServerTimeDeltaSum   int64
	serverTimeDeltaCron  *cron.Cron
	*SunxOrderBook
	*SunxKline
	*SunxDepth
	*SunxAggTrade
}

var SunxServerTimeDeltaActive bool = true

func SetSunxServerTimeDeltaActive(active bool) {
	SunxServerTimeDeltaActive = active
}

func (sm *SunxMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		if !SunxServerTimeDeltaActive {
			sm.ServerTimeDelta = 0
			sm.ServerTimeDeltaTimes = 0
			sm.ServerTimeDeltaSum = 0
			return
		}
		serverTimeDelta, err := SunxGetServerTimeDelta(SUNX_SWAP)
		if err != nil {
			log.Error(err)
		}
		//丢弃高波动均值影响
		if sm.ServerTimeDeltaTimes > 10 && sm.ServerTimeDelta != 0 {
			if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(3*sm.ServerTimeDelta)) {
				return
			}
		}
	}
	refresh()

	//每隔15秒更新一次服务器时间
	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	sm.serverTimeDeltaCron = c
	return nil
}

func NewSunxMarketDataDefault() (*SunxMarketData, error) {
	return NewSunxMarketData("", "")
}
func NewSunxMarketData(accessKey, secretKey string) (*SunxMarketData, error) {
	marketData := &SunxMarketData{
		Client: mysunxapi.Client{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}

func (sm *SunxMarketData) InitSunxOrderBook(config SunxOrderBookConfig) error {
	s := &SunxOrderBook{}
	s.SwapOrderBook = s.newSunxOrderBookBase(config.SwapConfig)
	s.SwapOrderBook.AccountType = SUNX_SWAP
	s.SwapOrderBook.parent = s
	s.init()
	sm.SunxOrderBook = s
	s.parent = sm
	return nil
}

func (sm *SunxMarketData) InitSunxKline(config SunxKlineConfig) error {
	s := &SunxKline{}
	s.SwapKline = s.newSunxKlineBase(config.SwapConfig)
	s.SwapKline.AccountType = SUNX_SWAP
	s.SwapKline.parent = s
	sm.SunxKline = s
	s.parent = sm
	return nil
}

func (sm *SunxMarketData) InitSunxDepth(config SunxDepthConfig) error {
	s := &SunxDepth{}
	s.SwapDepth = s.newSunxDepthBase(config.SwapConfig)
	s.SwapDepth.AccountType = SUNX_SWAP
	s.SwapDepth.parent = s
	sm.SunxDepth = s
	s.parent = sm
	return nil
}

func (sm *SunxMarketData) InitSunxAggTrade(config SunxAggTradeConfig) error {
	s := &SunxAggTrade{}
	s.SwapAggTrade = s.newSunxAggTradeBase(config.SwapConfig)
	s.SwapAggTrade.AccountType = SUNX_SWAP
	s.SwapAggTrade.parent = s
	sm.SunxAggTrade = s
	s.parent = sm
	return nil
}

// Ticker

// 获取当前服务器时间差
func (sm *SunxMarketData) GetServerTimeDelta() int64 {
	return sm.ServerTimeDelta
}
