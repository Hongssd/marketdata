package marketdata

type BinanceAggTradeConfig struct {
	SpotConfig   BinanceAggTradeConfigBase
	FutureConfig BinanceAggTradeConfigBase
	SwapConfig   BinanceAggTradeConfigBase
}

type BinanceAggTradeConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
}
