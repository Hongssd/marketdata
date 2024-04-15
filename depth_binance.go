package marketdata

import (
	"errors"
	"fmt"
	"github.com/Hongssd/mybinanceapi"
	"github.com/robfig/cron/v3"
	"sync/atomic"
	"time"
)

type BinanceDepth struct {
	parent      *BinanceMarketData
	SpotDepth   *binanceDepthBase
	FutureDepth *binanceDepthBase
	SwapDepth   *binanceDepthBase
}

type binanceDepthBase struct {
	serverTimeDelta int64
	parent          *BinanceDepth
	level           string //深度档位
	uSpeed          string //更新速度
	perConnSubNum   int64
	Exchange        Exchange
	AccountType     BinanceAccountType
	DepthMap        *MySyncMap[string, *Depth]                                           //symbol->last depth
	WsClientListMap *MySyncMap[*mybinanceapi.WsStreamClient, *int64]                     //ws client->subscribe count
	WsClientMap     *MySyncMap[string, *mybinanceapi.WsStreamClient]                     //symbol->ws client
	SubMap          *MySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]] //symbol->subscribe
	CallBackMap     *MySyncMap[string, func(depth *Depth, err error)]                    //symbol->callback
}

func (b *BinanceDepth) getBaseMapFromAccountType(accountType BinanceAccountType) (*binanceDepthBase, error) {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotDepth, nil
	case BINANCE_FUTURE:
		return b.FutureDepth, nil
	case BINANCE_SWAP:
		return b.SwapDepth, nil
	}
	return nil, ErrorAccountType
}

func (b *BinanceDepth) newBinanceDepthBase(config BinanceDepthConfigBase) *binanceDepthBase {
	return &binanceDepthBase{
		Exchange:        BINANCE,
		perConnSubNum:   config.PerConnSubNum,
		level:           config.Level,
		uSpeed:          config.USpeed,
		DepthMap:        GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientListMap: GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
		WsClientMap:     GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:          GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		CallBackMap:     GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

// 封装好的获取深度方法
func (b *BinanceDepth) GetLastDepth(BinanceAccountType BinanceAccountType, symbol string) (*Depth, error) {
	symbolKey := symbol

	bmap, err := b.getBaseMapFromAccountType(BinanceAccountType)
	if err != nil {
		return nil, err
	}
	depth, ok := bmap.DepthMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s depth not found", BinanceAccountType, symbol)
		return nil, err
	}
	return depth, nil
}

// 获取当前或新建ws客户端
func (b *BinanceDepth) GetCurrentOrNewWsClient(accountType BinanceAccountType) (*mybinanceapi.WsStreamClient, error) {
	var WsClientListMap *MySyncMap[*mybinanceapi.WsStreamClient, *int64]
	perConnSubNum := int64(0)
	switch accountType {
	case BINANCE_SPOT:
		WsClientListMap = b.SpotDepth.WsClientListMap
		perConnSubNum = b.SpotDepth.perConnSubNum
	case BINANCE_FUTURE:
		WsClientListMap = b.FutureDepth.WsClientListMap
		perConnSubNum = b.FutureDepth.perConnSubNum
	case BINANCE_SWAP:
		WsClientListMap = b.SwapDepth.WsClientListMap
		perConnSubNum = b.SwapDepth.perConnSubNum
	default:
		return nil, ErrorAccountType
	}

	// log.Info(WsClientList)

	var wsClient *mybinanceapi.WsStreamClient
	var err error
	WsClientListMap.Range(func(k *mybinanceapi.WsStreamClient, v *int64) bool {
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
		err = wsClient.OpenConn()
		if err != nil {
			return nil, err
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
func (b *binanceDepthBase) subscribeBinanceDepth(binanceWsClient *mybinanceapi.WsStreamClient, symbol string, callback func(depth *Depth, err error)) error {
	return b.subscribeBinanceDepthMultiple(binanceWsClient, []string{symbol}, callback)
}

// 订阅币安有限档深度底层执行
func (b *binanceDepthBase) subscribeBinanceDepthMultiple(binanceWsClient *mybinanceapi.WsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {

	binanceSub, err := binanceWsClient.SubscribeLevelDepthMultiple(symbols, b.level, b.uSpeed)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		symbolKey := symbol
		b.WsClientMap.Store(symbolKey, binanceWsClient)
		b.SubMap.Store(symbolKey, binanceSub)
		b.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-binanceSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-binanceSub.ResultChan():
				symbolKey := result.Symbol
				bids := []PriceLevel{}
				asks := []PriceLevel{}

				for _, bid := range result.Bids {
					bids = append(bids, PriceLevel{Price: bid.Price, Quantity: bid.Quantity})
				}
				for _, ask := range result.Asks {
					asks = append(asks, PriceLevel{Price: ask.Price, Quantity: ask.Quantity})
				}

				//保存至Depth
				depth := &Depth{
					Exchange:    b.Exchange.String(),
					AccountType: b.AccountType.String(),
					Symbol:      result.Symbol,
					Timestamp:   result.Timestamp,
					Bids:        bids,
					Asks:        asks,
				}
				b.DepthMap.Store(symbolKey, depth)
				if callback != nil {
					callback(depth, nil)
				}
			case <-binanceSub.CloseChan():
				log.Info("订阅已关闭: ", binanceSub.Params)
				return
			}
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := b.WsClientListMap.Load(binanceWsClient)
	if !ok {
		initCount := currentCount
		b.WsClientListMap.Store(binanceWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)

	return nil
}

// 取消订阅币安深度
func (b *binanceDepthBase) UnSubscribeBinanceDepth(symbol string) error {
	symbolKey := symbol
	binanceSub, ok := b.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return binanceSub.Unsubscribe()
}

// 订阅深度
func (b *BinanceDepth) SubscribeDepth(accountType BinanceAccountType, symbol string) error {
	return b.SubscribeDepthWithCallBack(accountType, symbol, nil)
}

// 批量订阅深度
func (b *BinanceDepth) SubscribeDepths(accountType BinanceAccountType, symbols []string) error {
	return b.SubscribeDepthsWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (b *BinanceDepth) SubscribeDepthWithCallBack(accountType BinanceAccountType, symbol string, callback func(depth *Depth, err error)) error {
	client, err := b.GetCurrentOrNewWsClient(accountType)
	if err != nil {
		return err
	}

	switch accountType {
	case BINANCE_SPOT:
		err = b.SpotDepth.subscribeBinanceDepth(client, symbol, callback)
	case BINANCE_FUTURE:
		err = b.FutureDepth.subscribeBinanceDepth(client, symbol, callback)
	case BINANCE_SWAP:
		err = b.SwapDepth.subscribeBinanceDepth(client, symbol, callback)
	}

	return err
}

// 批量订阅深度并带上回调
func (b *BinanceDepth) SubscribeDepthsWithCallBack(accountType BinanceAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅有限档深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentBinanceDepthBase *binanceDepthBase

	switch accountType {
	case BINANCE_SPOT:
		currentBinanceDepthBase = b.SpotDepth
	case BINANCE_FUTURE:
		currentBinanceDepthBase = b.FutureDepth
	case BINANCE_SWAP:
		currentBinanceDepthBase = b.SwapDepth
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := 100
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
			err = currentBinanceDepthBase.subscribeBinanceDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}

			currentCount, ok := currentBinanceDepthBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("深度%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)

			time.Sleep(1000 * time.Millisecond)
		}

	} else {
		client, err := b.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentBinanceDepthBase.subscribeBinanceDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

// 获取当前服务器时间差
func (b *BinanceDepth) GetServerTimeDelta(accountType BinanceAccountType) int64 {
	switch accountType {
	case BINANCE_SPOT:
		return b.SpotDepth.serverTimeDelta
	case BINANCE_FUTURE:
		return b.FutureDepth.serverTimeDelta
	case BINANCE_SWAP:
		return b.SwapDepth.serverTimeDelta
	}
	return 0
}

// 初始化
func (b *BinanceDepth) init() {
	mybinanceapi.SetLogger(log)
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		b.SpotDepth.RefreshDelta()
		b.FutureDepth.RefreshDelta()
		b.SwapDepth.RefreshDelta()
	}
	refresh()

	//每隔5秒更新一次服务器时间
	_, err := c.AddFunc("*/5 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return
	}
	c.Start()
}

func (b *binanceDepthBase) RefreshDelta() {
	switch b.AccountType {
	case BINANCE_SPOT:
		b.serverTimeDelta = b.parent.parent.spotServerTimeDelta
	case BINANCE_FUTURE:
		b.serverTimeDelta = b.parent.parent.futureServerTimeDelta
	case BINANCE_SWAP:
		b.serverTimeDelta = b.parent.parent.swapServerTimeDelta
	}
}
