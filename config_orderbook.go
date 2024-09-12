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
	PerSubMaxLen              int    //每条链接每次订阅的最大数量
	CallBackDepthLevel        int64  //回调深度档位
	CallBackDepthTimeoutMilli int64  //超时毫秒数
	InitOrderBookSize         int    //初始OrderBook档位
}

type OkxOrderBookConfig struct {
	WsBooksType               myokxapi.WsBooksType
	PerConnSubNum             int64 //每条链接订阅的数量
	PerSubMaxLen              int   //每条链接每次订阅的最大数量
	CallBackDepthLevel        int64 //回调深度档位
	CallBackDepthTimeoutMilli int64 //超时毫秒数
}

type OkxOptionBookConfig struct {
	WsBooksType               myokxapi.WsBooksType
	PerConnSubNum             int64 //每条链接订阅的数量
	PerSubMaxLen              int   //每条链接每次订阅的最大数量
	CallBackDepthTimeoutMilli int64 //超时毫秒数
}

type BybitOrderBookConfig struct {
	SpotConfig    BybitOrderBookConfigBase
	LinearConfig  BybitOrderBookConfigBase
	InverseConfig BybitOrderBookConfigBase
}

type BybitOrderBookConfigBase struct {
	PerConnSubNum             int64  //每条链接订阅的数量
	PerSubMaxLen              int    //每条链接每次订阅的最大数量
	CallBackDepthLevel        int64  //回调深度档位
	CallBackDepthTimeoutMilli int64  //超时毫秒数
	Level                     string //初始OrderBook档位
}
