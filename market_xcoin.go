package marketdata

import (
	"errors"
	"math"

	"github.com/Hongssd/myxcoinapi"
	"github.com/robfig/cron/v3"
)

type XcoinMarketData struct {
	myxcoinapi.Client
	serverTimeDelta      int64
	serverTimeDeltaTimes int64
	serverTimeDeltaSum   int64
	serverTimeDeltaCron  *cron.Cron
	*XcoinOrderBook
	*XcoinKline
	*XcoinDepth
	*XcoinAggTrade
	*XcoinTickers
}

var XcoinServerTimeDeltaActive bool = true

func SetXcoinServerTimeDeltaActive(active bool) {
	XcoinServerTimeDeltaActive = active
}

func NewXcoinMarketDataDefault() (*XcoinMarketData, error) {
	return NewXcoinMarketData("", "")
}
func NewXcoinMarketData(apiKey, apiSecret string) (*XcoinMarketData, error) {
	marketData := &XcoinMarketData{
		Client: myxcoinapi.Client{
			APIKey:    apiKey,
			APISecret: apiSecret,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}

func (xm *XcoinMarketData) InitXcoinOrderBook(config XcoinOrderBookConfig) error {
	o := xm.newXcoinOrderBook(config)
	xm.XcoinOrderBook = o
	return nil
}

func (xm *XcoinMarketData) InitXcoinKline(config XcoinKlineConfig) error {
	k := xm.newXcoinKline(config)
	xm.XcoinKline = k
	return nil
}

func (xm *XcoinMarketData) InitXcoinDepth(config XcoinDepthConfig) error {
	d := xm.newXcoinDepth(config)
	xm.XcoinDepth = d
	return nil
}

func (xm *XcoinMarketData) InitXcoinAggTrade(config XcoinAggTradeConfig) error {
	a := xm.newXcoinAggTrade(config)
	xm.XcoinAggTrade = a
	return nil
}

func (xm *XcoinMarketData) InitXcoinTickers(config XcoinTickersConfig) error {
	t := xm.newXcoinTickers(config)
	xm.XcoinTickers = t
	return nil
}

func (xm *XcoinMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		if !XcoinServerTimeDeltaActive {
			xm.serverTimeDelta = 0
			xm.serverTimeDeltaTimes = 0
			xm.serverTimeDeltaSum = 0
			return
		}
		serverTimeDelta, err := XcoinGetServerTimeDelta()
		if err != nil {
			log.Error(err)
			return
		}
		//丢弃高波动均值影响
		if xm.serverTimeDeltaTimes > 10 && xm.serverTimeDelta != 0 {
			if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(3*xm.serverTimeDelta)) {
				return
			}
		}
		xm.serverTimeDeltaTimes++
		xm.serverTimeDeltaSum += serverTimeDelta
		xm.serverTimeDelta = xm.serverTimeDeltaSum / xm.serverTimeDeltaTimes
	}
	refresh()

	//每隔15秒更新一次服务器时间
	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	xm.serverTimeDeltaCron = c
	return nil
}

func (xm *XcoinMarketData) GetPublicCurrentOrNewWsClient(perConnSubNum int64, wsClientListMap *MySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]) (*myxcoinapi.PublicWsStreamClient, error) {
	if wsClientListMap == nil {
		return nil, errors.New("wsClientListMap is nil")
	}
	var wsClient *myxcoinapi.PublicWsStreamClient
	var err error
	wsClientListMap.Range(func(k *myxcoinapi.PublicWsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient == nil {
		wsClient = xcoin.NewPublicWsStreamClient()
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		initCount := int64(0)
		wsClientListMap.Store(wsClient, &initCount)
		if wsClientListMap.Length() > 1 {
			log.Infof("当前链接订阅权重已用完，建立新的Ws链接，当前链接数:%d ...", wsClientListMap.Length())
		} else {
			log.Info("首次建立新的Ws链接...")
		}
	}
	return wsClient, nil
}

// 获取当前服务器时间差
func (xm *XcoinMarketData) GetServerTimeDelta() int64 {
	return xm.serverTimeDelta
}

func (xm *XcoinMarketData) Close() {
	if xm.serverTimeDeltaCron != nil {
		xm.serverTimeDeltaCron.Stop()
	}
}
