package marketdata

import "github.com/Hongssd/myasterapi"

type AsterWsClientBase struct {
	perConnSubNum   int64
	perSubMaxLen    int
	WsClientListMap *MySyncMap[*myasterapi.WsStreamClient, *int64] //ws client->subscribe count
}

func (b *AsterWsClientBase) GetCurrentOrNewWsClient(accountType AsterAccountType) (*myasterapi.WsStreamClient, error) {
	perConnSubNum := b.perConnSubNum
	var wsClient *myasterapi.WsStreamClient
	var err error
	b.WsClientListMap.Range(func(k *myasterapi.WsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}

		return true
	})

	if wsClient == nil {
		//新建链接
		switch accountType {
		case ASTER_SPOT:
			wsClient = &aster.NewSpotWsStreamClient().WsStreamClient
		case ASTER_FUTURE:
			wsClient = &aster.NewFutureWsStreamClient().WsStreamClient
		}
		//开启链接
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
		}
		log.Infof("新建ws客户端成功，账户类型:%s", accountType)
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

func (b *AsterWsClientBase) close() {
	b.WsClientListMap.Range(func(k *myasterapi.WsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	b.WsClientListMap.Clear()
}
