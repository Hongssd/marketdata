package marketdata

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mysunxapi"
)

type SunxDepth struct {
	parent    *SunxMarketData
	SwapDepth *sunxDepthBase
}

type sunxDepthBase struct {
	parent *SunxDepth
	level  string //深度档位
	SunxWsClientBase
	Exchange    Exchange
	AccountType SunxAccountType
	DepthMap    *MySyncMap[string, *Depth]
	WsClientMap *MySyncMap[string, *mysunxapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketCommonReq, mysunxapi.WsDepth]]
	CallBackMap *MySyncMap[string, func(depth *Depth, err error)]
}

func (s *SunxDepth) getBaseMapFromAccountType(accountType SunxAccountType) (*sunxDepthBase, error) {
	switch accountType {
	case SUNX_SWAP:
		return s.SwapDepth, nil
	}
	return nil, ErrorAccountType
}

func (s *SunxDepth) newSunxDepthBase(config SunxDepthConfigBase) *sunxDepthBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	return &sunxDepthBase{
		Exchange: SUNX,
		SunxWsClientBase: SunxWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mysunxapi.PublicWsStreamClient, *int64]()),
		},
		level:       config.Level,
		DepthMap:    GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mysunxapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketCommonReq, mysunxapi.WsDepth]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

// 封装好的获取深度方法
func (s *SunxDepth) GetLastDepth(SunxAccountType SunxAccountType, symbol string) (*Depth, error) {
	symbolKey := symbol

	bmap, err := s.getBaseMapFromAccountType(SunxAccountType)
	if err != nil {
		return nil, err
	}
	depth, ok := bmap.DepthMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s depth not found", SunxAccountType, symbol)
		return nil, err
	}
	return depth, nil
}

// 获取当前或新建ws客户端
func (s *SunxDepth) GetCurrentOrNewWsClient(accountType SunxAccountType) (*mysunxapi.PublicWsStreamClient, error) {
	switch accountType {
	case SUNX_SWAP:
		return s.SwapDepth.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (s *sunxDepthBase) subscribeSunxDepth(sunxWsClient *mysunxapi.PublicWsStreamClient, symbol string, callback func(depth *Depth, err error)) error {
	return s.subscribeSunxDepthMultiple(sunxWsClient, []string{symbol}, callback)
}

// 订阅sunx有限档深度底层执行
func (s *sunxDepthBase) subscribeSunxDepthMultiple(sunxWsClient *mysunxapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {
	sunxSub, err := sunxWsClient.SubscribeMarketDepth(symbols, []string{s.level}, true)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		s.WsClientMap.Store(symbolKey, sunxWsClient)
		s.SubMap.Store(symbolKey, sunxSub)
		s.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-sunxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-sunxSub.ResultChan():
				split := strings.Split(result.Ch, ".")
				if len(split) < 2 {
					log.Error("subscribe err, r.ch could be empty")
					continue
				}
				symbol := split[1]
				symbolKey := symbol
				bids := []PriceLevel{}
				asks := []PriceLevel{}

				for _, bid := range result.Tick.Bids {
					bids = append(bids, PriceLevel{Price: bid.Price, Quantity: bid.Volume})
				}
				for _, ask := range result.Tick.Asks {
					asks = append(asks, PriceLevel{Price: ask.Price, Quantity: ask.Volume})
				}

				UId, PreUId := s.GetUidAndPreUid(result, symbolKey)
				now := time.Now().UnixMilli()
				targetTs := result.Tick.Ts + s.parent.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				//保存至Depth
				depth := &Depth{
					UId:         UId,
					PreUId:      PreUId,
					Exchange:    s.Exchange.String(),
					AccountType: s.AccountType.String(),
					Symbol:      symbol,
					Timestamp:   targetTs,
					Bids:        bids,
					Asks:        asks,
				}
				s.DepthMap.Store(symbolKey, depth)
				if callback != nil {
					callback(depth, nil)
				}
			case <-sunxSub.CloseChan():
				log.Info("订阅已关闭: ", sunxSub.SubReqs)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := s.WsClientListMap.Load(sunxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		s.WsClientListMap.Store(sunxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅sunx深度
func (s *sunxDepthBase) UnSubscribeSunxDepth(symbol string) error {
	symbolKey := symbol
	sunxSub, ok := s.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return sunxSub.Unsubscribe()
}

func (s *sunxDepthBase) GetUidAndPreUid(result mysunxapi.WsDepth, symbol string) (int64, int64) {
	UId := int64(0)
	PreUId := int64(0)

	UId = result.Tick.Ts

	if lastDepth, ok := s.DepthMap.Load(symbol); ok && lastDepth != nil {
		PreUId = lastDepth.UId
	} else {
		PreUId = 0
	}

	return UId, PreUId
}

// 订阅深度
func (s *SunxDepth) SubscribeDepth(accountType SunxAccountType, symbol string) error {
	return s.SubscribeDepthWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (s *SunxDepth) SubscribeDepths(accountType SunxAccountType, symbols []string) error {
	return s.SubscribeDepthsWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (s *SunxDepth) SubscribeDepthWithCallBack(accountType SunxAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return s.SubscribeDepthsWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅深度并带上回调
func (s *SunxDepth) SubscribeDepthsWithCallBack(accountType SunxAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅有限档深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentSunxDepthBase *sunxDepthBase

	switch accountType {
	case SUNX_SWAP:
		currentSunxDepthBase = s.SwapDepth
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentSunxDepthBase.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := s.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentSunxDepthBase.subscribeSunxDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentSunxDepthBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("有限档深度%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := s.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentSunxDepthBase.subscribeSunxDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("有限档深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (s *sunxDepthBase) Close() {
	s.SunxWsClientBase.close()

	s.DepthMap.Clear()
	s.WsClientMap.Clear()
	s.SubMap.Clear()
	s.CallBackMap.Clear()
}

func (s *SunxDepth) Close() {
	s.SwapDepth.Close()
}
