package marketdata

import (
	"github.com/Hongssd/mybinanceapi"
	"github.com/robfig/cron/v3"
	"sync"
)

type BinanceMarketData struct {
	mybinanceapi.Client
	spotServerTimeDelta   int64
	futureServerTimeDelta int64
	swapServerTimeDelta   int64
	*BinanceOrderBook
	*BinanceKline
	*BinanceDepth
	*BinanceAggTrade
}

func (bm *BinanceMarketData) init() error {
	c := cron.New(cron.WithSeconds())
	refresh := func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			serverTimeDelta, err := BinanceGetServerTimeDelta(BINANCE_SPOT)
			if err != nil {
				log.Error(err)
			}
			bm.spotServerTimeDelta = serverTimeDelta
		}()
		go func() {
			defer wg.Done()
			serverTimeDelta, err := BinanceGetServerTimeDelta(BINANCE_FUTURE)
			if err != nil {
				log.Error(err)
			}
			bm.futureServerTimeDelta = serverTimeDelta
		}()
		go func() {
			defer wg.Done()
			serverTimeDelta, err := BinanceGetServerTimeDelta(BINANCE_SWAP)
			if err != nil {
				log.Error(err)
			}
			bm.swapServerTimeDelta = serverTimeDelta
		}()
		wg.Wait()
	}
	refresh()

	//每隔15秒更新一次服务器时间
	_, err := c.AddFunc("*/15 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	return nil
}

func NewBinanceMarketDataDefault() (*BinanceMarketData, error) {
	return NewBinanceMarketData("", "")
}
func NewBinanceMarketData(apiKey, apiSecrey string) (*BinanceMarketData, error) {
	marketData := &BinanceMarketData{
		Client: mybinanceapi.Client{
			ApiKey:    apiKey,
			ApiSecret: apiSecrey,
		},
	}
	err := marketData.init()
	if err != nil {
		return nil, err
	}
	return marketData, nil
}
func (bm *BinanceMarketData) InitBinanceOrderBook(config BinanceOrderBookConfig) error {
	b := &BinanceOrderBook{}
	b.SpotOrderBook = b.newBinanceOrderBookBase(config.SpotConfig)
	b.SpotOrderBook.AccountType = BINANCE_SPOT
	b.SpotOrderBook.parent = b
	b.FutureOrderBook = b.newBinanceOrderBookBase(config.FutureConfig)
	b.FutureOrderBook.AccountType = BINANCE_FUTURE
	b.FutureOrderBook.parent = b
	b.SwapOrderBook = b.newBinanceOrderBookBase(config.SwapConfig)
	b.SwapOrderBook.AccountType = BINANCE_SWAP
	b.SwapOrderBook.parent = b
	b.init()
	bm.BinanceOrderBook = b
	b.parent = bm
	return nil
}

func (bm *BinanceMarketData) InitBinanceKline(config BinanceKlineConfig) error {
	b := &BinanceKline{}
	b.SpotKline = b.newBinanceKlineBase(config.SpotConfig)
	b.SpotKline.AccountType = BINANCE_SPOT
	b.SpotKline.parent = b
	b.FutureKline = b.newBinanceKlineBase(config.FutureConfig)
	b.FutureKline.AccountType = BINANCE_FUTURE
	b.FutureKline.parent = b
	b.SwapKline = b.newBinanceKlineBase(config.SwapConfig)
	b.SwapKline.AccountType = BINANCE_SWAP
	b.SwapKline.parent = b
	bm.BinanceKline = b
	b.parent = bm
	return nil
}

func (bm *BinanceMarketData) InitBinanceDepth(config BinanceDepthConfig) error {
	b := &BinanceDepth{}
	b.SpotDepth = b.newBinanceDepthBase(config.SpotConfig)
	b.SpotDepth.AccountType = BINANCE_SPOT
	b.SpotDepth.parent = b
	b.FutureDepth = b.newBinanceDepthBase(config.FutureConfig)
	b.FutureDepth.AccountType = BINANCE_FUTURE
	b.FutureDepth.parent = b
	b.SwapDepth = b.newBinanceDepthBase(config.SwapConfig)
	b.SwapDepth.AccountType = BINANCE_SWAP
	b.SwapDepth.parent = b
	bm.BinanceDepth = b
	b.parent = bm
	return nil
}

func (bm *BinanceMarketData) InitBinanceAggTrade(config BinanceAggTradeConfig) error {
	b := &BinanceAggTrade{}
	b.SpotAggTrade = b.newBinanceAggTradeBase(config.SpotConfig)
	b.SpotAggTrade.AccountType = BINANCE_SPOT
	b.SpotAggTrade.parent = b
	b.FutureAggTrade = b.newBinanceAggTradeBase(config.FutureConfig)
	b.FutureAggTrade.AccountType = BINANCE_FUTURE
	b.FutureAggTrade.parent = b
	b.SwapAggTrade = b.newBinanceAggTradeBase(config.SwapConfig)
	b.SwapAggTrade.AccountType = BINANCE_SWAP
	b.SwapAggTrade.parent = b
	bm.BinanceAggTrade = b
	b.parent = bm
	return nil
}

// 获取当前服务器时间差
func (bm *BinanceMarketData) GetServerTimeDelta(accountType BinanceAccountType) int64 {
	switch accountType {
	case BINANCE_SPOT:
		return bm.spotServerTimeDelta
	case BINANCE_FUTURE:
		return bm.futureServerTimeDelta
	case BINANCE_SWAP:
		return bm.swapServerTimeDelta
	default:
		return 0
	}
}
