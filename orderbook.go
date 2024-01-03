package marketdata

type BinanceOrderBookConfig struct {
	SpotConfig   BinanceOrderBookConfigBase
	FutureConfig BinanceOrderBookConfigBase
	SwapConfig   BinanceOrderBookConfigBase
}

type BinanceOrderBookConfigBase struct {
	USpeed                  string //深度更新速度
	LimitRestCountPerMinute int64  //每分钟请求次数
	PerConnSubNum           int64  //每条链接订阅的数量
}
