package marketdata

import (
	"github.com/Hongssd/mybinanceapi"
	"github.com/Hongssd/myokxapi"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var log = logrus.New()
var binance = mybinanceapi.MyBinance{}
var okx = myokxapi.MyOkx{}

func SetLogger(logger *logrus.Logger) {
	log = logger
}

type BinanceMarketData struct {
	*BinanceOrderBook
}

func NewBinanceMarketData() *BinanceMarketData {
	return &BinanceMarketData{}
}
func (bm *BinanceMarketData) InitBinanceOrderBook(config BinanceOrderBookConfig) error {
	b := &BinanceOrderBook{}
	b.SpotOrderBook = b.newBinanceOrderBookBase(config.SpotConfig)
	b.SpotOrderBook.AccountType = BINANCE_SPOT
	b.FutureOrderBook = b.newBinanceOrderBookBase(config.FutureConfig)
	b.FutureOrderBook.AccountType = BINANCE_FUTURE
	b.SwapOrderBook = b.newBinanceOrderBookBase(config.SwapConfig)
	b.SwapOrderBook.AccountType = BINANCE_SWAP
	b.init()
	bm.BinanceOrderBook = b
	return nil
}
