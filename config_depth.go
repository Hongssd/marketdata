package marketdata

type BinanceDepthConfig struct {
	SpotConfig   BinanceDepthConfigBase
	FutureConfig BinanceDepthConfigBase
	SwapConfig   BinanceDepthConfigBase
}

type BinanceDepthConfigBase struct {
	PerConnSubNum int64  //每条链接订阅的数量
	PerSubMaxLen  int    //每条链接每次订阅的最大数量
	Level         string //深度档位
	USpeed        string //深度更新速度
}

type GateDepthConfig struct {
	SpotConfig     GateDepthConfigBase
	FuturesConfig  GateDepthConfigBase
	DeliveryConfig GateDepthConfigBase
}

type GateDepthConfigBase struct {
	PerConnSubNum int64  //每条链接订阅的数量
	PerSubMaxLen  int    //每条链接每次订阅的最大数量
	Level         string //深度档位
	USpeed        string //深度更新速度
}
