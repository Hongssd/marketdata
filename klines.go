package marketdata

type BinanceKlineConfig struct {
	SpotConfig   BinanceKlineConfigBase
	FutureConfig BinanceKlineConfigBase
	SwapConfig   BinanceKlineConfigBase
}

type BinanceKlineConfigBase struct {
	PerConnSubNum int64 //每条链接订阅的数量
}

type BinanceDepthConfig struct {
	SpotConfig   BinanceDepthConfigBase
	FutureConfig BinanceDepthConfigBase
	SwapConfig   BinanceDepthConfigBase
}

type BinanceDepthConfigBase struct {
	PerConnSubNum int64  //每条链接订阅的数量
	Level         string //深度档位
	USpeed        string //深度更新速度
}
