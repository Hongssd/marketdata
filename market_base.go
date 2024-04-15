package marketdata

import (
	"github.com/Hongssd/mybinanceapi"
	"github.com/Hongssd/myokxapi"
	jsoniter "github.com/json-iterator/go"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"sync"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var log = logrus.New()
var binance = mybinanceapi.MyBinance{}
var okx = myokxapi.MyOkx{}

func SetLogger(logger *logrus.Logger) {
	log = logger
}

type BinanceMarketData struct {
	mybinanceapi.Client
	spotServerTimeDelta   int64
	futureServerTimeDelta int64
	swapServerTimeDelta   int64
	*BinanceOrderBook
	*BinanceKline
	*BinanceDepth
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
	b.init()
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
	b.init()
	return nil
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

	//每隔5秒更新一次服务器时间
	_, err := c.AddFunc("*/5 * * * * *", refresh)
	if err != nil {
		log.Error(err)
		return err
	}
	c.Start()
	return nil
}

type OkxMarketData struct {
	myokxapi.Client
	*OkxOrderBook
}

func NewOkxMarketDataDefault() *OkxMarketData {
	return NewOkxMarketData("", "", "")
}
func NewOkxMarketData(APIKey, SecretKey, Passphrase string) *OkxMarketData {
	return &OkxMarketData{
		Client: myokxapi.Client{
			APIKey:     APIKey,
			SecretKey:  SecretKey,
			Passphrase: Passphrase,
		},
	}
}
func (om *OkxMarketData) InitOkxOrderBook(config OkxOrderBookConfig) error {
	o := om.newOkxOrderBook(config)
	om.OkxOrderBook = o
	o.init()
	return nil
}
