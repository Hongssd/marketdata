package marketdata

import "github.com/Hongssd/mygateapi"

type GateWsClientBase struct {
	perConnSubNum   int64
	perSubMaxLen    int
	WsClientListMap *MySyncMap[*mygateapi.WsStreamClient, *int64] //ws client->subscribe count
}

func (b *GateWsClientBase) GetCurrentOrNewWsClient(accountType GateAccountType) (*mygateapi.WsStreamClient, error) {
	perConnSubNum := b.perConnSubNum
	var wsClient *mygateapi.WsStreamClient
	var err error
	b.WsClientListMap.Range(func(k *mygateapi.WsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}

		return true
	})

	if wsClient == nil {
		//新建链接
		switch accountType {
		case GATE_SPOT:
			wsClient = &mygateapi.NewSpotWsStreamClient(mygateapi.NewRestClient("", "")).WsStreamClient
		case GATE_FUTURES:
			wsClient = &mygateapi.NewFuturesWsStreamClient(mygateapi.NewRestClient("", ""), mygateapi.USDT_CONTRACT).WsStreamClient
		case GATE_DELIVERY:
			wsClient = &mygateapi.NewDeliveryWsStreamClient(mygateapi.NewRestClient("", ""), mygateapi.USDT_CONTRACT).WsStreamClient
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

func (b *GateWsClientBase) close() {
	b.WsClientListMap.Range(func(k *mygateapi.WsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	b.WsClientListMap.Clear()
}
