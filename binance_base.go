package marketdata

import "github.com/Hongssd/mybinanceapi"

type BinanceWsClientBase struct {
	perConnSubNum   int64
	perSubMaxLen    int
	WsClientListMap *MySyncMap[*mybinanceapi.WsStreamClient, *int64] //ws client->subscribe count
}

func (b *BinanceWsClientBase) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {
	perConnSubNum := b.perConnSubNum
	var wsClient *mybinanceapi.WsStreamClient
	var err error
	b.WsClientListMap.Range(func(k *mybinanceapi.WsStreamClient, v *int64) bool {
		if *v < perConnSubNum {
			wsClient = k
			return false
		}

		return true
	})

	if wsClient == nil {
		//新建链接
		switch accountType {
		case BINANCE_SPOT:
			wsClient = &binance.NewSpotWsStreamClient().WsStreamClient
		case BINANCE_FUTURE:
			wsClient = &binance.NewFutureWsStreamClient().WsStreamClient
		case BINANCE_SWAP:
			wsClient = &binance.NewSwapWsStreamClient().WsStreamClient
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

func (b *BinanceWsClientBase) close() {
	b.WsClientListMap.Range(func(k *mybinanceapi.WsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	b.WsClientListMap.Clear()
}
