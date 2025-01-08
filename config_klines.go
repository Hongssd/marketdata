package marketdata

type BinanceKlineConfig struct {
	SpotConfig   BinanceKlineConfigBase
	FutureConfig BinanceKlineConfigBase
	SwapConfig   BinanceKlineConfigBase
}

type BinanceKlineConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}

type OkxKlineConfig struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}

type BybitKlineConfig struct {
	SpotConfig    BybitKlineConfigBase
	LinearConfig  BybitKlineConfigBase
	InverseConfig BybitKlineConfigBase
}

type BybitKlineConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}

type GateKlineConfig struct {
	SpotConfig     GateKlineConfigBase
	FuturesConfig  GateKlineConfigBase
	DeliveryConfig GateKlineConfigBase
}

type GateKlineConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}
