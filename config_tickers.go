package marketdata

type BinanceTickersConfig struct {
	SpotConfig   BinanceTickersConfigBase
	FutureConfig BinanceTickersConfigBase
	SwapConfig   BinanceTickersConfigBase
}

type BinanceTickersConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
} 