package marketdata

import (
	"github.com/Hongssd/mybitgetapi"
)

type BitgetWsClientBase struct {
	perConnSubNum   int64
	perSubMaxLen    int
	WsClientListMap *MySyncMap[*mybitgetapi.PublicWsStreamClient, *int64]
}

// BitgetAccountTypeToInstType 将业务账户类型映射为 mybitgetapi.InstType（Subscribe* 内部会 ToLower 为 ws 所需小写串）。
func BitgetAccountTypeToInstType(accountType BitgetAccountType) (mybitgetapi.InstType, error) {
	switch accountType {
	case BITGET_SPOT:
		return mybitgetapi.INST_TYPE_SPOT, nil
	case BITGET_USDT_FUTURES:
		return mybitgetapi.INST_TYPE_USDT_FUTURES, nil
	case BITGET_COIN_FUTURES:
		return mybitgetapi.INST_TYPE_COIN_FUTURES, nil
	case BITGET_USDC_FUTURES:
		return mybitgetapi.INST_TYPE_USDC_FUTURES, nil
	default:
		return "", ErrorAccountType
	}
}

func (b *BitgetWsClientBase) GetCurrentOrNewWsClient() (*mybitgetapi.PublicWsStreamClient, error) {
	var wsClient *mybitgetapi.PublicWsStreamClient
	var err error
	b.WsClientListMap.Range(func(k *mybitgetapi.PublicWsStreamClient, v *int64) bool {
		if *v < b.perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient == nil {
		wsClient = bitget.NewPublicWsStreamClient()
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

func (b *BitgetWsClientBase) close() {
	b.WsClientListMap.Range(func(k *mybitgetapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})
	b.WsClientListMap.Clear()
}
