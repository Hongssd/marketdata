package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mybitgetapi"
)

type BitgetDepth struct {
	parent             *BitgetMarketData
	SpotDepth          *bitgetDepthBase
	UsdtFuturesDepth   *bitgetDepthBase
	CoinFuturesDepth   *bitgetDepthBase
	UsdcFuturesDepth   *bitgetDepthBase
}

type bitgetDepthBase struct {
	parent *BitgetDepth
	BitgetWsClientBase
	Exchange    Exchange
	AccountType BitgetAccountType
	InstType    mybitgetapi.InstType
	booksType   mybitgetapi.WsBooksType
	DepthMap    *MySyncMap[string, *Depth]
	WsClientMap *MySyncMap[string, *mybitgetapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsBooks]]
	CallBackMap *MySyncMap[string, func(depth *Depth, err error)]
}

func normalizeBitgetDepthConfigBase(c *BitgetDepthConfigBase) {
	if c.PerSubMaxLen == 0 {
		c.PerSubMaxLen = 50
	}
	if c.PerConnSubNum == 0 {
		c.PerConnSubNum = 50
	}
	if c.BooksType == "" {
		c.BooksType = mybitgetapi.WS_BOOKS_5
	}
}

func (bm *BitgetMarketData) newBitgetDepth(config BitgetDepthConfig) *BitgetDepth {
	normalizeBitgetDepthConfigBase(&config.SpotConfig)
	normalizeBitgetDepthConfigBase(&config.UsdtFuturesConfig)
	normalizeBitgetDepthConfigBase(&config.CoinFuturesConfig)
	normalizeBitgetDepthConfigBase(&config.UsdcFuturesConfig)
	b := &BitgetDepth{parent: bm}
	b.SpotDepth = b.newBitgetDepthBase(config.SpotConfig, BITGET_SPOT, config.SpotConfig.BooksType)
	b.SpotDepth.parent = b
	b.UsdtFuturesDepth = b.newBitgetDepthBase(config.UsdtFuturesConfig, BITGET_USDT_FUTURES, config.UsdtFuturesConfig.BooksType)
	b.UsdtFuturesDepth.parent = b
	b.CoinFuturesDepth = b.newBitgetDepthBase(config.CoinFuturesConfig, BITGET_COIN_FUTURES, config.CoinFuturesConfig.BooksType)
	b.CoinFuturesDepth.parent = b
	b.UsdcFuturesDepth = b.newBitgetDepthBase(config.UsdcFuturesConfig, BITGET_USDC_FUTURES, config.UsdcFuturesConfig.BooksType)
	b.UsdcFuturesDepth.parent = b
	return b
}

func (b *BitgetDepth) newBitgetDepthBase(config BitgetDepthConfigBase, accountType BitgetAccountType, booksType mybitgetapi.WsBooksType) *bitgetDepthBase {
	instType, _ := BitgetAccountTypeToInstType(accountType)
	return &bitgetDepthBase{
		BitgetWsClientBase: BitgetWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mybitgetapi.PublicWsStreamClient, *int64]()),
		},
		Exchange:    BITGET,
		AccountType: accountType,
		InstType:    instType,
		booksType:   booksType,
		DepthMap:    GetPointer(NewMySyncMap[string, *Depth]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mybitgetapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mybitgetapi.Subscription[mybitgetapi.WsBooks]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

func (b *BitgetDepth) getBaseFromAccountType(accountType BitgetAccountType) (*bitgetDepthBase, error) {
	switch accountType {
	case BITGET_SPOT:
		return b.SpotDepth, nil
	case BITGET_USDT_FUTURES:
		return b.UsdtFuturesDepth, nil
	case BITGET_COIN_FUTURES:
		return b.CoinFuturesDepth, nil
	case BITGET_USDC_FUTURES:
		return b.UsdcFuturesDepth, nil
	default:
		return nil, ErrorAccountType
	}
}

func (b *BitgetDepth) GetLastDepth(accountType BitgetAccountType, symbol string) (*Depth, error) {
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return nil, err
	}
	d, ok := base.DepthMap.Load(symbol)
	if !ok {
		return nil, fmt.Errorf("%s symbol:%s depth not found", accountType, symbol)
	}
	return d, nil
}

func (d *bitgetDepthBase) wsBooksToDepth(result mybitgetapi.WsBooks) *Depth {
	bids := make([]PriceLevel, 0, len(result.Bids))
	asks := make([]PriceLevel, 0, len(result.Asks))
	for _, bid := range result.Bids {
		bids = append(bids, PriceLevel{Price: stringToFloat64(bid.Price), Quantity: stringToFloat64(bid.Quantity)})
	}
	for _, ask := range result.Asks {
		asks = append(asks, PriceLevel{Price: stringToFloat64(ask.Price), Quantity: stringToFloat64(ask.Quantity)})
	}
	now := time.Now().UnixMilli()
	ts := result.PushTs + d.parent.parent.GetServerTimeDelta()
	if result.Ts != "" {
		if t, err := strconv.ParseInt(result.Ts, 10, 64); err == nil {
			ts = t + d.parent.parent.GetServerTimeDelta()
		}
	}
	if ts > now {
		ts = now
	}
	return &Depth{
		UId:         result.Seq,
		PreUId:      result.PSeq,
		Exchange:    d.Exchange.String(),
		AccountType: d.AccountType.String(),
		Symbol:      result.Symbol,
		Timestamp:   ts,
		Bids:        bids,
		Asks:        asks,
	}
}

func (d *bitgetDepthBase) subscribeBitgetDepthMultiple(wsClient *mybitgetapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {
	sub, err := wsClient.SubscribeBooksMultiple(d.InstType, symbols, d.booksType)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		d.WsClientMap.Store(symbol, wsClient)
		d.SubMap.Store(symbol, sub)
		d.CallBackMap.Store(symbol, callback)
	}
	go func() {
		for {
			select {
			case err := <-sub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-sub.ResultChan():
				if result.Symbol == "" {
					continue
				}
				depth := d.wsBooksToDepth(result)
				d.DepthMap.Store(result.Symbol, depth)
				cb, _ := d.CallBackMap.Load(result.Symbol)
				if cb != nil {
					cb(depth, nil)
				}
			case <-sub.CloseChan():
				log.Info("Bitget depth 订阅已关闭: ", sub.Args)
				return
			}
		}
	}()
	currentCount := int64(len(symbols))
	count, ok := d.WsClientListMap.Load(wsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		d.WsClientListMap.Store(wsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (b *BitgetDepth) SubscribeDepth(accountType BitgetAccountType, symbol string) error {
	return b.SubscribeDepthWithCallBack(accountType, symbol, nil)
}

func (b *BitgetDepth) SubscribeDepths(accountType BitgetAccountType, symbols []string) error {
	return b.SubscribeDepthsWithCallBack(accountType, symbols, nil)
}

func (b *BitgetDepth) SubscribeDepthWithCallBack(accountType BitgetAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return b.SubscribeDepthsWithCallBack(accountType, []string{symbol}, callback)
}

func (b *BitgetDepth) SubscribeDepthsWithCallBack(accountType BitgetAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅 Bitget 有限档深度，accountType:%s，交易对数:%d", accountType, len(symbols))
	base, err := b.getBaseFromAccountType(accountType)
	if err != nil {
		return err
	}
	LEN := base.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			temp := symbols[i:end]
			client, err := base.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			if err = base.subscribeBitgetDepthMultiple(client, temp, callback); err != nil {
				return err
			}
			currentCount, ok := base.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Bitget 深度分批订阅成功，交易对:%v，当前链接订阅数:%d，等待1秒...", temp, *currentCount)
			time.Sleep(time.Second)
		}
	} else {
		client, err := base.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		if err = base.subscribeBitgetDepthMultiple(client, symbols, callback); err != nil {
			return err
		}
	}
	log.Infof("Bitget 有限档深度订阅结束，accountType:%s", accountType)
	return nil
}

func (d *bitgetDepthBase) Close() {
	d.BitgetWsClientBase.close()
	d.DepthMap.Clear()
	d.WsClientMap.Clear()
	d.SubMap.Clear()
	d.CallBackMap.Clear()
}

func (b *BitgetDepth) Close() {
	b.SpotDepth.Close()
	b.UsdtFuturesDepth.Close()
	b.CoinFuturesDepth.Close()
	b.UsdcFuturesDepth.Close()
}
