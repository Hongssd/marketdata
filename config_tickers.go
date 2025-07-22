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

type OkxTickersConfig struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}

type BybitTickersConfig struct {
	SpotConfig    BybitTickersConfigBase
	LinearConfig  BybitTickersConfigBase
	InverseConfig BybitTickersConfigBase
	OptionConfig  BybitTickersConfigBase
}

type BybitTickersConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
} 