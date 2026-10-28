package marketdata

import (
	"math"

	"github.com/Hongssd/mypolymarketapi"
	"github.com/robfig/cron/v3"
)

type PolymarketMarketData struct {
	mypolymarketapi.Client
	serverTimeDelta      int64
	serverTimeDeltaTimes int64
	serverTimeDeltaSum   int64
	serverTimeDeltaCron  *cron.Cron
	*PolymarketOrderBook
	*PolymarketAggTrade
}

var PolymarketServerTimeDeltaActive bool = true

func SetPolymarketServerTimeDeltaActive(active bool) {
	PolymarketServerTimeDeltaActive = active
}

func NewPolymarketMarketDataDefault() (*PolymarketMarketData, error) {
	return NewPolymarketMarketData()
}

func NewPolymarketMarketData() (*PolymarketMarketData, error) {
	marketData := &PolymarketMarketData{
		Client: mypolymarketapi.Client{},
	}
	if err := marketData.init(); err != nil {
		return nil, err
	}
	return marketData, nil
}

func (pm *PolymarketMarketData) InitPolymarketOrderBook(config PolymarketOrderBookConfig) error {
	pm.PolymarketOrderBook = NewPolymarketOrderBook(config)
	return nil
}

func (pm *PolymarketMarketData) InitPolymarketAggTrade(config PolymarketAggTradeConfig) error {
	pm.PolymarketAggTrade = pm.newPolymarketAggTrade(config)
	return nil
}

// GetServerTimeDelta 返回由 CLOB /time 定时采样得到的缓存时差（毫秒），不在此处发起 HTTP。
func (pm *PolymarketMarketData) GetServerTimeDelta() int64 {
	return pm.serverTimeDelta
}

func (pm *PolymarketMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		if !PolymarketServerTimeDeltaActive {
			pm.serverTimeDelta = 0
			pm.serverTimeDeltaTimes = 0
			pm.serverTimeDeltaSum = 0
			return
		}
		serverTimeDelta, err := PolymarketGetServerTimeDelta()
		if err != nil {
			log.Error(err)
			return
		}
		if pm.serverTimeDeltaTimes > 10 && pm.serverTimeDelta != 0 {
			if math.Abs(float64(serverTimeDelta)) > math.Abs(float64(3*pm.serverTimeDelta)) {
				return
			}
		}
		pm.serverTimeDeltaTimes++
		pm.serverTimeDeltaSum += serverTimeDelta
		pm.serverTimeDelta = pm.serverTimeDeltaSum / pm.serverTimeDeltaTimes
	}
	refresh()

	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	pm.serverTimeDeltaCron = c
	return nil
}

func (pm *PolymarketMarketData) GetCurrentOrNewWsClient(
	perConnSubNum int64,
	wsClientListMap *MySyncMap[*mypolymarketapi.MarketWsStreamClient, *int64],
	level int,
) (*mypolymarketapi.MarketWsStreamClient, error) {
	var wsClient *mypolymarketapi.MarketWsStreamClient

	wsClientListMap.Range(func(k *mypolymarketapi.MarketWsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient != nil {
		return wsClient, nil
	}

	pmClient := mypolymarketapi.MyPolymarket{}
	wsClient = pmClient.NewMarketWsStreamClient()
	wsClient.SetLevel(level)
	err := wsClient.OpenConn()
	if err != nil {
		return nil, err
	}

	initCount := int64(0)
	wsClientListMap.Store(wsClient, &initCount)
	if wsClientListMap.Length() > 1 {
		log.Infof("当前链接订阅权重已用完，建立新的Polymarket Ws链接，当前链接数:%d ...", wsClientListMap.Length())
	} else {
		log.Info("首次建立新的Polymarket Ws链接...")
	}
	return wsClient, nil
}

func (pm *PolymarketMarketData) Close() {
	if pm.serverTimeDeltaCron != nil {
		pm.serverTimeDeltaCron.Stop()
	}
	if pm.PolymarketOrderBook != nil {
		pm.PolymarketOrderBook.Close()
	}
	if pm.PolymarketAggTrade != nil {
		pm.PolymarketAggTrade.Close()
	}
}
