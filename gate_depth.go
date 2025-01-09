package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mygateapi"
	"github.com/shopspring/decimal"
	"sync/atomic"
	"time"
)

type GateDepth struct {
	parent        *GateMarketData
	SpotDepth     *gateDepthBase
	FuturesDepth  *gateDepthBase
	DeliveryDepth *gateDepthBase
}

type gateDepthBase struct {
	parent *GateDepth
	level  string //深度档位
	uSpeed string //更新速度
	GateWsClientBase
	Exchange    Exchange
	AccountType GateAccountType
	DepthMap    *MySyncMap[string, *Depth]                                                                              //symbol->last depth
	WsClientMap *MySyncMap[string, *mygateapi.WsStreamClient]                                                           //symbol->ws client
	SubMap      *MySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsOrderBook]]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(depth *Depth, err error)]                                                       //symbol->callback
}

func (b *GateDepth) getBaseMapFromAccountType(accountType GateAccountType) (*gateDepthBase, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotDepth, nil
	case GATE_FUTURES:
		return b.FuturesDepth, nil
	case GATE_DELIVERY:
		return b.DeliveryDepth, nil
	}
	return nil, ErrorAccountType
}

func (b *GateDepth) newGateDepthBase(config GateDepthConfigBase) *gateDepthBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	return &gateDepthBase{
		Exchange: GATE,
		GateWsClientBase: GateWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mygateapi.WsStreamClient, *int64]()),
		},
		level:       config.Level,
		uSpeed:      config.USpeed,
		DepthMap:    GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mygateapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mygateapi.MultipleSubscription[mygateapi.WsSubscribeResult[mygateapi.WsOrderBook]]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

// 封装好的获取深度方法
func (b *GateDepth) GetLastDepth(GateAccountType GateAccountType, symbol string) (*Depth, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(GateAccountType)
	if err != nil {
		return nil, err
	}
	depth, ok := bmap.DepthMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s depth not found", GateAccountType, symbol)
		return nil, err
	}
	return depth, nil
}

// 获取当前或新建ws客户端
func (b *GateDepth) GetCurrentOrNewWsClient(accountType GateAccountType) (*mygateapi.WsStreamClient, error) {
	switch accountType {
	case GATE_SPOT:
		return b.SpotDepth.GetCurrentOrNewWsClient(accountType)
	case GATE_FUTURES:
		return b.FuturesDepth.GetCurrentOrNewWsClient(accountType)
	case GATE_DELIVERY:
		return b.DeliveryDepth.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (b *gateDepthBase) subscribeGateDepth(gateWsClient *mygateapi.WsStreamClient, symbol string, callback func(depth *Depth, err error)) error {
	return b.subscribeGateDepthMultiple(gateWsClient, []string{symbol}, callback)
}

// 订阅币安有限档深度底层执行
func (b *gateDepthBase) subscribeGateDepthMultiple(gateWsClient *mygateapi.WsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {

	gateSub, err := gateWsClient.SubscribeOrderBookMultiple(symbols, b.uSpeed, b.level)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, gateWsClient)
		b.SubMap.Store(symbolKey, gateSub)
		b.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-gateSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-gateSub.ResultChan():
				result := r.Result
				symbolKey := result.Symbol
				bids := []PriceLevel{}
				asks := []PriceLevel{}

				for _, bid := range result.Bids {
					p, _ := decimal.NewFromString(bid.Price)
					q, _ := decimal.NewFromString(bid.Quantity)
					bids = append(bids, PriceLevel{Price: p.InexactFloat64(), Quantity: q.InexactFloat64()})
				}
				for _, ask := range result.Asks {
					p, _ := decimal.NewFromString(ask.Price)
					q, _ := decimal.NewFromString(ask.Quantity)
					asks = append(asks, PriceLevel{Price: p.InexactFloat64(), Quantity: q.InexactFloat64()})
				}

				UId, PreUId := b.GetUidAndPreUid(*result)
				//保存至Depth
				depth := &Depth{
					UId:         UId,
					PreUId:      PreUId,
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   result.Timestamp + b.parent.parent.GetServerTimeDelta(),
					Bids:        bids,
					Asks:        asks,
				}
				b.DepthMap.Store(symbolKey, depth)
				if callback != nil {
					callback(depth, nil)
				}
			case <-gateSub.CloseChan():
				log.Info("订阅已关闭: ", gateSub.SubKeys)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(gateWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(gateWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安深度
func (b *gateDepthBase) UnSubscribeGateDepth(symbol string) error {
	symbolKey := symbol
	gateSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return gateSub.Unsubscribe()
}
func (b *gateDepthBase) GetUidAndPreUid(result mygateapi.WsOrderBook) (int64, int64) {
	UId := result.Id
	PreUId := result.Id - 1
	return UId, PreUId
}

// 订阅深度
func (b *GateDepth) SubscribeDepth(accountType GateAccountType, symbol string) error {
	return b.SubscribeDepthWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *GateDepth) SubscribeDepths(accountType GateAccountType, symbols []string) error {
	return b.SubscribeDepthsWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (b *GateDepth) SubscribeDepthWithCallBack(accountType GateAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeDepthsWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅深度并带上回调
func (b *GateDepth) SubscribeDepthsWithCallBack(accountType GateAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅有限档深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentGateDepthBase *gateDepthBase

	switch accountType {
	case GATE_SPOT:
		currentGateDepthBase = b.SpotDepth
	case GATE_FUTURES:
		currentGateDepthBase = b.FuturesDepth
	case GATE_DELIVERY:
		currentGateDepthBase = b.DeliveryDepth
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentGateDepthBase.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := b.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentGateDepthBase.subscribeGateDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentGateDepthBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("有限档深度%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentGateDepthBase.subscribeGateDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("有限档深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *gateDepthBase) Close() {
	b.GateWsClientBase.close()

	b.DepthMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *GateDepth) Close() {
	b.SpotDepth.Close()
	b.FuturesDepth.Close()
	b.DeliveryDepth.Close()
}
