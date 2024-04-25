package marketdata

import (
	"github.com/Hongssd/myokxapi"
	"github.com/robfig/cron/v3"
)

type OkxMarketData struct {
	myokxapi.Client
	serverTimeDelta int64
	*OkxOrderBook
	*OkxKline
	*OkxAggTrade
}

func NewOkxMarketDataDefault() (*OkxMarketData, error) {
	return NewOkxMarketData("", "", "")
}
func NewOkxMarketData(APIKey, SecretKey, Passphrase string) (*OkxMarketData, error) {
	marketData := &OkxMarketData{
		Client: myokxapi.Client{
			APIKey:     APIKey,
			SecretKey:  SecretKey,
			Passphrase: Passphrase,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}
func (om *OkxMarketData) InitOkxOrderBook(config OkxOrderBookConfig) error {
	o := om.newOkxOrderBook(config)
	om.OkxOrderBook = o
	return nil
}
func (om *OkxMarketData) InitOkxKline(config OkxKlineConfig) error {
	o := om.newOkxKline(config)
	om.OkxKline = o
	return nil
}
func (om *OkxMarketData) InitOkxAggTrade(config OkxAggTradeConfig) error {
	o := om.newOkxAggTrade(config)
	om.OkxAggTrade = o
	return nil
}

// 获取当前服务器时间差
func (om *OkxMarketData) GetServerTimeDelta() int64 {
	return om.serverTimeDelta
}

// 获取当前服务器时间差
func (om *OkxMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		serverTimeDelta, err := OkxGetServerTimeDelta()
		if err != nil {
			log.Error(err)
		}
		om.serverTimeDelta = serverTimeDelta
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

func (om *OkxMarketData) isWsNeedLogin() bool {
	if om.APIKey == "" || om.SecretKey == "" || om.Passphrase == "" {
		return false
	}
	return true
}

func (o *OkxMarketData) GetPublicCurrentOrNewWsClient(perConnSubNum int64, WsClientListMap *MySyncMap[*myokxapi.PublicWsStreamClient, *int64]) (*myokxapi.PublicWsStreamClient, error) {
	var wsClient *myokxapi.PublicWsStreamClient
	var err error
	WsClientListMap.Range(func(k *myokxapi.PublicWsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient == nil {
		//新建链接
		wsClient = okx.NewPublicWsStreamClient()
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		if o.isWsNeedLogin() {
			err = wsClient.Login(okx.NewRestClient(o.APIKey, o.SecretKey, o.Passphrase))
			if err != nil {
				log.Error(err)
				return nil, err
			}
			log.Info("ws登录成功")
		}

		initCount := int64(0)
		WsClientListMap.Store(wsClient, &initCount)
		if WsClientListMap.Length() > 1 {
			log.Infof("当前链接订阅权重已用完，建立新的Ws链接，当前链接数:%d ...", WsClientListMap.Length())
		} else {
			log.Info("首次建立新的Ws链接...")
		}
	}
	return wsClient, nil
}

func (o *OkxMarketData) GetBusinessCurrentOrNewWsClient(perConnSubNum int64, WsClientListMap *MySyncMap[*myokxapi.BusinessWsStreamClient, *int64]) (*myokxapi.BusinessWsStreamClient, error) {
	var wsClient *myokxapi.BusinessWsStreamClient
	var err error
	WsClientListMap.Range(func(k *myokxapi.BusinessWsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient == nil {
		//新建链接
		wsClient = okx.NewBusinessWsStreamClient()
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		if o.isWsNeedLogin() {
			err = wsClient.Login(okx.NewRestClient(o.APIKey, o.SecretKey, o.Passphrase))
			if err != nil {
				log.Error(err)
				return nil, err
			}
			log.Info("ws登录成功")
		}

		initCount := int64(0)
		WsClientListMap.Store(wsClient, &initCount)
		if WsClientListMap.Length() > 1 {
			log.Infof("当前链接订阅权重已用完，建立新的Ws链接，当前链接数:%d ...", WsClientListMap.Length())
		} else {
			log.Info("首次建立新的Ws链接...")
		}
	}
	return wsClient, nil
}
