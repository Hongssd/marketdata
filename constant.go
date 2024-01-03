package marketdata

import "errors"

var ErrorAccountType = errors.New("account type error")

type Exchange string

const (
	BINANCE Exchange = "BINANCE"
	OKX     Exchange = "OKX"
)

type BinanceAccountType string

const (
	BINANCE_SPOT   BinanceAccountType = "SPOT"
	BINANCE_FUTURE BinanceAccountType = "FUTURE"
	BINANCE_SWAP   BinanceAccountType = "SWAP"
)
