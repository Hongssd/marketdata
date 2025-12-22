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

type AsterOrderBookConfig struct {
	SpotConfig   AsterOrderBookConfigBase
	FutureConfig AsterOrderBookConfigBase
}

type AsterOrderBookConfigBase struct {
	USpeed                    string //深度更新速度
	LimitRestCountPerMinute   int64  //每分钟请求次数
	PerConnSubNum             int64  //每条链接订阅的数量
	PerSubMaxLen              int    //每条链接每次订阅的最大数量
	CallBackDepthLevel        int64  //回调深度档位
	CallBackDepthTimeoutMilli int64  //超时毫秒数
	InitOrderBookSize         int    //初始OrderBook档位
}

type SunxOrderBookConfig struct {
	SwapConfig SunxOrderBookConfigBase
}

type SunxOrderBookConfigBase struct {
	Level                     int   //深度档位
	LimitRestCountPerMinute   int64 //每分钟请求次数
	PerConnSubNum             int64 //每条链接订阅的数量
	PerSubMaxLen              int   //每条链接每次订阅的最大数量
	CallBackDepthLevel        int64 //回调深度档位
	CallBackDepthTimeoutMilli int64 //超时毫秒数
	InitOrderBookSize         int   //初始OrderBook档位
}

var BinanceOrderBookConfigDefault = BinanceOrderBookConfig{
	SpotConfig: BinanceOrderBookConfigBase{
		USpeed:                    "100ms",
		LimitRestCountPerMinute:   500,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
	},
	FutureConfig: BinanceOrderBookConfigBase{
		USpeed:                    "100ms",
		LimitRestCountPerMinute:   500,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
	},
	SwapConfig: BinanceOrderBookConfigBase{
		USpeed:                    "100ms",
		LimitRestCountPerMinute:   500,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
	},
}

var OkxOrderBookConfigDefault = OkxOrderBookConfig{
	WsBooksType:               myokxapi.WS_BOOKS_UPDATE_400_100MS,
	PerConnSubNum:             20,
	PerSubMaxLen:              20,
	CallBackDepthLevel:        20,
	CallBackDepthTimeoutMilli: 5000,
}

var BybitOrderBookConfigDefault = BybitOrderBookConfig{
	SpotConfig: BybitOrderBookConfigBase{
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		Level:                     "50",
	},
	LinearConfig: BybitOrderBookConfigBase{
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		Level:                     "50",
	},
	InverseConfig: BybitOrderBookConfigBase{
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		Level:                     "50",
	},
}

var GateOrderBookConfigDefault = GateOrderBookConfig{
	SpotConfig: GateOrderBookConfigBase{
		USpeed:                    "100ms",
		LimitRestCountPerMinute:   300,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
		Level:                     "20",
	},
	FuturesConfig: GateOrderBookConfigBase{
		USpeed:                    "10ms",
		LimitRestCountPerMinute:   300,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
		Level:                     "20",
	},
	DeliveryConfig: GateOrderBookConfigBase{
		USpeed:                    "10ms",
		LimitRestCountPerMinute:   300,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
		Level:                     "20",
	},
}

var AsterOrderBookConfigDefault = AsterOrderBookConfig{
	SpotConfig: AsterOrderBookConfigBase{
		USpeed:                    "100ms",
		LimitRestCountPerMinute:   500,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
	},
	FutureConfig: AsterOrderBookConfigBase{
		USpeed:                    "100ms",
		LimitRestCountPerMinute:   500,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
	},
}

var SunxOrderBookConfigDefault = SunxOrderBookConfig{
	SwapConfig: SunxOrderBookConfigBase{
		Level:                     20,
		LimitRestCountPerMinute:   500,
		PerConnSubNum:             20,
		PerSubMaxLen:              20,
		CallBackDepthLevel:        20,
		CallBackDepthTimeoutMilli: 5000,
		InitOrderBookSize:         100,
	},
}
