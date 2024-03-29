package marketdata

import "github.com/Hongssd/myokxapi"

type BinanceOrderBookConfig struct {
	SpotConfig   BinanceOrderBookConfigBase
	FutureConfig BinanceOrderBookConfigBase
	SwapConfig   BinanceOrderBookConfigBase
}

type BinanceOrderBookConfigBase struct {
	USpeed                    string //深度更新速度
	LimitRestCountPerMinute   int64  //每分钟请求次数
	PerConnSubNum             int64  //每条链接订阅的数量
	CallBackDepthLevel        int64  //回调深度档位
	CallBackDepthTimeoutMilli int64  //超时毫秒数
}

type OkxOrderBookConfig struct {
	WsBooksType               myokxapi.WsBooksType
	PerConnSubNum             int64 //每条链接订阅的数量
	CallBackDepthLevel        int64 //回调深度档位
	CallBackDepthTimeoutMilli int64 //超时毫秒数
}
