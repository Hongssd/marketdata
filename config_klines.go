package marketdata

type BinanceKlineConfig struct {
	SpotConfig   BinanceKlineConfigBase
	FutureConfig BinanceKlineConfigBase
	SwapConfig   BinanceKlineConfigBase
}

type BinanceKlineConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
}
