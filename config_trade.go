package marketdata

type BinanceTradeConfig struct {
	SpotConfig   BinanceTradeConfigBase
	FutureConfig BinanceTradeConfigBase
	SwapConfig   BinanceTradeConfigBase
}

type BinanceTradeConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}
type OkxTradeConfig struct {
	PerConnSubNum int64 //每条链接订阅的数量
	PerSubMaxLen  int   //每条链接每次订阅的最大数量
}
