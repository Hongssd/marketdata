package marketdata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myxcoinapi"
)

type XcoinDepth struct {
	parent          *XcoinMarketData
	level           myxcoinapi.WsDepthLevelsType         //深度档位
	uSpeed          myxcoinapi.WsDepthLevelsIntervalType //深度更新速度
	perConnSubNum   int64
	perSubMaxLen    int
	Exchange        Exchange
	BusinessType    XcoinBusinessType
	DepthMap        *MySyncMap[string, *Depth]
	WsClientListMap *MySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *myxcoinapi.PublicWsStreamClient]
	SubMap          *MySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsDepthLevels]]
	CallBackMap     *MySyncMap[string, func(depth *Depth, err error)]
}

func (xm *XcoinMarketData) newXcoinDepth(config XcoinDepthConfig) *XcoinDepth {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	return &XcoinDepth{
		parent:          xm,
		level:           config.Level,
		uSpeed:          config.USpeed,
		perConnSubNum:   config.PerConnSubNum,
		perSubMaxLen:    config.PerSubMaxLen,
		Exchange:        XCOIN,
		DepthMap:        GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientListMap: GetPointer(NewMySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *myxcoinapi.PublicWsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsDepthLevels]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

// 封装好的获取深度方法
func (x *XcoinDepth) GetLastDepth(businessType XcoinBusinessType, symbol string) (*Depth, error) {
	symbolKey := symbol

	depth, ok := x.DepthMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("symbol:%s depth not found", symbol)
		return nil, err
	}
	return depth, nil
}

func (x *XcoinDepth) GetCurrentOrNewWsClient() (*myxcoinapi.PublicWsStreamClient, error) {
	return x.parent.GetPublicCurrentOrNewWsClient(x.perConnSubNum, x.WsClientListMap)
}

func (x *XcoinDepth) subscribeXcoinDepth(client *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbol string, callback func(depth *Depth, err error)) error {
	return x.subscribeXcoinDepthMultiple(client, businessType, []string{symbol}, callback)
}

// 订阅Xcoin有限档深度底层执行
func (x *XcoinDepth) subscribeXcoinDepthMultiple(client *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbols []string, callback func(depth *Depth, err error)) error {
	xcoinSub, err := client.SubscribeDepthLevelsMulti(businessType.String(), x.uSpeed, x.level, "1", symbols)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		x.WsClientMap.Store(symbolKey, client)
		x.SubMap.Store(symbolKey, xcoinSub)
		x.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-xcoinSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-xcoinSub.ResultChan():
				symbolKey := result.Symbol
				bids := []PriceLevel{}
				asks := []PriceLevel{}

				for _, bid := range result.Bids {
					bids = append(bids, PriceLevel{
						Price:    stringToFloat64(bid.Price),
						Quantity: stringToFloat64(bid.Quantity),
					})
				}
				for _, ask := range result.Asks {
					asks = append(asks, PriceLevel{
						Price:    stringToFloat64(ask.Price),
						Quantity: stringToFloat64(ask.Quantity),
					})
				}

				UId, PreUId := x.GetUidAndPreUid(result)
				now := time.Now().UnixMilli()
				targetTs := result.Ts + x.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				//保存至Depth
				depth := &Depth{
					UId:         UId,
					PreUId:      PreUId,
					Exchange:    x.Exchange.String(),
					AccountType: businessType.String(),
					Symbol:      result.Symbol,
					Timestamp:   targetTs,
					Bids:        bids,
					Asks:        asks,
				}
				x.DepthMap.Store(symbolKey, depth)
				if callback != nil {
					callback(depth, nil)
				}
			case <-xcoinSub.CloseChan():
				log.Info("订阅已关闭: ", xcoinSub.Args)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := x.WsClientListMap.Load(client)
	if !ok {
		initCount := int64(0)
		count = &initCount
		x.WsClientListMap.Store(client, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

func (x *XcoinDepth) GetUidAndPreUid(result myxcoinapi.WsDepthLevels) (int64, int64) {
	UId := int64(0)
	PreUId := int64(0)

	UId = stringToInt64(result.LastUpdateId)

	if lastDepth, ok := x.DepthMap.Load(result.Symbol); ok && lastDepth != nil {
		PreUId = lastDepth.UId
	} else {
		PreUId = 0
	}

	return UId, PreUId
}

// 订阅深度
func (x *XcoinDepth) SubscribeDepth(businessType XcoinBusinessType, symbol string) error {
	return x.SubscribeDepthWithCallBack(businessType, symbol, nil)
}

// 批量订阅深度
func (x *XcoinDepth) SubscribeDepths(businessType XcoinBusinessType, symbols []string) error {
	return x.SubscribeDepthMultipleWithCallBack(businessType, symbols, nil)
}

// 订阅深度并带上回调
func (x *XcoinDepth) SubscribeDepthWithCallBack(businessType XcoinBusinessType, symbol string, callback func(depth *Depth, err error)) error {
	return x.SubscribeDepthMultipleWithCallBack(businessType, []string{symbol}, callback)
}

func (x *XcoinDepth) SubscribeDepthMultipleWithCallBack(businessType XcoinBusinessType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅有限档深度%s，交易对数:%d, 总订阅数:%d", businessType, len(symbols), len(symbols))

	//订阅总数超过LEN次，分批订阅
	LEN := x.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := x.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = x.subscribeXcoinDepthMultiple(client, businessType, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := x.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("有限档深度%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", businessType, tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := x.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = x.subscribeXcoinDepthMultiple(client, businessType, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("有限档深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (x *XcoinDepth) Close() {
	x.WsClientListMap.Range(func(k *myxcoinapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})

	x.DepthMap.Clear()
	x.WsClientListMap.Clear()
	x.WsClientMap.Clear()
	x.SubMap.Clear()
	x.CallBackMap.Clear()
}
