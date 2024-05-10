package marketdata

import "github.com/Hongssd/mybybitapi"

type BybitWsClientBase struct {
	perConnSubNum   int64
	perSubMaxLen    int
	WsClientListMap *MySyncMap[*mybybitapi.PublicWsStreamClient, *int64] //ws client->subscribe count
}

func (b *BybitWsClientBase) GetCurrentOrNewWsClient(accountType BybitAccountType) (*mybybitapi.PublicWsStreamClient, error) {
	perConnSubNum := b.perConnSubNum
	var wsClient *mybybitapi.PublicWsStreamClient
	var err error
	b.WsClientListMap.Range(func(k *mybybitapi.PublicWsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}

		return true
	})

	if wsClient == nil {
		//新建链接
		switch accountType {
		case BYBIT_SPOT:
			wsClient = mybybitapi.NewPublicSpotWsStreamClient()
		case BYBIT_LINEAR:
			wsClient = mybybitapi.NewPublicLinearWsStreamClient()
		case BYBIT_INVERSE:
			wsClient = mybybitapi.NewPublicInverseWsStreamClient()
		case BYBIT_OPTION:
			wsClient = mybybitapi.NewPublicOptionWsStreamClient()
		}
		//开启链接
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		initCount := int64(0)
		b.WsClientListMap.Store(wsClient, &initCount)
		if b.WsClientListMap.Length() > 1 {
			log.Infof("当前链接订阅权重已用完，建立新的Ws链接，当前链接数:%d ...", b.WsClientListMap.Length())
		} else {
			log.Info("首次建立新的Ws链接...")
		}
	}
	return wsClient, nil
}
