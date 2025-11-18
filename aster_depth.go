package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myasterapi"
)

type AsterDepth struct {
	parent      *AsterMarketData
	SpotDepth   *asterDepthBase
	FutureDepth *asterDepthBase
}

type asterDepthBase struct {
	parent *AsterDepth
	level  string //深度档位
	uSpeed string //更新速度
	AsterWsClientBase
	Exchange    Exchange
	AccountType AsterAccountType
	DepthMap    *MySyncMap[string, *Depth]                                       //symbol->last depth
	WsClientMap *MySyncMap[string, *myasterapi.WsStreamClient]                   //symbol->ws client
	SubMap      *MySyncMap[string, *myasterapi.Subscription[myasterapi.WsDepth]] //symbol->subscribe
	CallBackMap *MySyncMap[string, func(depth *Depth, err error)]                //symbol->callback
}

func (b *AsterDepth) getBaseMapFromAccountType(accountType AsterAccountType) (*asterDepthBase, error) {
	switch accountType {
	case ASTER_SPOT:
		return b.SpotDepth, nil
	case ASTER_FUTURE:
		return b.FutureDepth, nil
	}
	return nil, ErrorAccountType
}

func (b *AsterDepth) newAsterDepthBase(config AsterDepthConfigBase) *asterDepthBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	return &asterDepthBase{
		Exchange: ASTER,
		AsterWsClientBase: AsterWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*myasterapi.WsStreamClient, *int64]()),
		},
		level:       config.Level,
		uSpeed:      config.USpeed,
		DepthMap:    GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *myasterapi.WsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *myasterapi.Subscription[myasterapi.WsDepth]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

// 封装好的获取深度方法
func (b *AsterDepth) GetLastDepth(AsterAccountType AsterAccountType, symbol string) (*Depth, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(AsterAccountType)
	if err != nil {
		return nil, err
	}
	depth, ok := bmap.DepthMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s depth not found", AsterAccountType, symbol)
		return nil, err
	}
	return depth, nil
}

// 获取当前或新建ws客户端
func (b *AsterDepth) GetCurrentOrNewWsClient(accountType AsterAccountType) (*myasterapi.WsStreamClient, error) {
	switch accountType {
	case ASTER_SPOT:
		return b.SpotDepth.GetCurrentOrNewWsClient(accountType)
	case ASTER_FUTURE:
		return b.FutureDepth.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (b *asterDepthBase) subscribeAsterDepth(asterWsClient *myasterapi.WsStreamClient, symbol string, callback func(depth *Depth, err error)) error {
	return b.subscribeAsterDepthMultiple(asterWsClient, []string{symbol}, callback)
}

// 订阅币安有限档深度底层执行
func (b *asterDepthBase) subscribeAsterDepthMultiple(asterWsClient *myasterapi.WsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {

	asterSub, err := asterWsClient.SubscribeLevelDepthMultiple(symbols, b.level, b.uSpeed)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, asterWsClient)
		b.SubMap.Store(symbolKey, asterSub)
		b.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-asterSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-asterSub.ResultChan():
				symbolKey := result.Symbol
				bids := []PriceLevel{}
				asks := []PriceLevel{}

				for _, bid := range result.Bids {
					bids = append(bids, PriceLevel{Price: bid.Price, Quantity: bid.Quantity})
				}
				for _, ask := range result.Asks {
					asks = append(asks, PriceLevel{Price: ask.Price, Quantity: ask.Quantity})
				}

				UId, PreUId := b.GetUidAndPreUid(result)
				now := time.Now().UnixMilli()
				targetTs := result.Timestamp + b.parent.parent.GetServerTimeDelta(b.AccountType)
				if targetTs > now {
					targetTs = now
				}
				//保存至Depth
				depth := &Depth{
					UId:         UId,
					PreUId:      PreUId,
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   targetTs,
					Bids:        bids,
					Asks:        asks,
				}
				b.DepthMap.Store(symbolKey, depth)
				if callback != nil {
					callback(depth, nil)
				}
			case <-asterSub.CloseChan():
				log.Info("订阅已关闭: ", asterSub.Params)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(asterWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		b.WsClientListMap.Store(asterWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安深度
func (b *asterDepthBase) UnSubscribeAsterDepth(symbol string) error {
	symbolKey := symbol
	asterSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return asterSub.Unsubscribe()
}
func (b *asterDepthBase) GetUidAndPreUid(result myasterapi.WsDepth) (int64, int64) {
	UId := int64(0)
	if result.LastUpdateID != 0 {
		UId = result.LastUpdateID
	} else if result.LowerU != 0 {
		UId = result.LowerU
	}
	PreUId := int64(0)

	PreUId = result.PreU

	return UId, PreUId
}

// 订阅深度
func (b *AsterDepth) SubscribeDepth(accountType AsterAccountType, symbol string) error {
	return b.SubscribeDepthWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *AsterDepth) SubscribeDepths(accountType AsterAccountType, symbols []string) error {
	return b.SubscribeDepthsWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (b *AsterDepth) SubscribeDepthWithCallBack(accountType AsterAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeDepthsWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅深度并带上回调
func (b *AsterDepth) SubscribeDepthsWithCallBack(accountType AsterAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅有限档深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentAsterDepthBase *asterDepthBase

	switch accountType {
	case ASTER_SPOT:
		currentAsterDepthBase = b.SpotDepth
	case ASTER_FUTURE:
		currentAsterDepthBase = b.FutureDepth
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentAsterDepthBase.perSubMaxLen
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
			err = currentAsterDepthBase.subscribeAsterDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentAsterDepthBase.WsClientListMap.Load(client)
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
		err = currentAsterDepthBase.subscribeAsterDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("有限档深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (b *asterDepthBase) Close() {
	b.AsterWsClientBase.close()

	b.DepthMap.Clear()
	b.WsClientMap.Clear()
	b.SubMap.Clear()
	b.CallBackMap.Clear()

}

func (b *AsterDepth) Close() {
	b.SpotDepth.Close()
	b.FutureDepth.Close()
}
