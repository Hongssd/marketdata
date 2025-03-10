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

type GateOrderBookConfig struct {
	SpotConfig     GateOrderBookConfigBase
	FuturesConfig  GateOrderBookConfigBase
	DeliveryConfig GateOrderBookConfigBase
}

type GateOrderBookConfigBase struct {
	USpeed                    string //深度更新速度 20ms or 100ms
	LimitRestCountPerMinute   int64  //每分钟请求次数
	PerConnSubNum             int64  //每条链接订阅的数量
	PerSubMaxLen              int    //每条链接每次订阅的最大数量
	CallBackDepthLevel        int64  //回调深度档位
	CallBackDepthTimeoutMilli int64  //超时毫秒数
	InitOrderBookSize         int    //初始OrderBook档位
	Level                     string //可选深度档位 100、50、20；20ms频率 只支持 20档位
}
