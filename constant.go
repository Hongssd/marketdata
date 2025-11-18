package marketdata

import "errors"

var ErrorAccountType = errors.New("account type error")

type Exchange string

const (
	BINANCE Exchange = "BINANCE"
	OKX     Exchange = "OKX"
	BYBIT   Exchange = "BYBIT"
	GATE    Exchange = "GATE"
	ASTER   Exchange = "ASTER"
)

func (e Exchange) String() string {
	return string(e)
}

type BinanceAccountType string

const (
	BINANCE_SPOT   BinanceAccountType = "SPOT"
	BINANCE_FUTURE BinanceAccountType = "FUTURE"
	BINANCE_SWAP   BinanceAccountType = "SWAP"
)

func (bat BinanceAccountType) String() string {
	return string(bat)
}

type OkxAccountType string

const (
	OKX_SPOT   OkxAccountType = "SPOT"
	OKX_FUTURE OkxAccountType = "SWAP"
	OKX_SWAP   OkxAccountType = "FUTURES"
	OKX_OPTION OkxAccountType = "OPTION"
)

func (oat OkxAccountType) String() string {
	return string(oat)
}

type BybitAccountType string

const (
	BYBIT_SPOT    BybitAccountType = "spot"
	BYBIT_LINEAR  BybitAccountType = "linear"
	BYBIT_INVERSE BybitAccountType = "inverse"
	BYBIT_OPTION  BybitAccountType = "option"
)

func (bat BybitAccountType) String() string {
	return string(bat)
}

type GateAccountType string

const (
	GATE_SPOT     GateAccountType = "spot"     //gate现货
	GATE_FUTURES  GateAccountType = "futures"  //gate永续
	GATE_DELIVERY GateAccountType = "delivery" //gate交割
)

func (bat GateAccountType) String() string {
	return string(bat)
}

type AsterAccountType string

const (
	ASTER_SPOT   AsterAccountType = "SPOT"
	ASTER_FUTURE AsterAccountType = "FUTURE"
)

func (aat AsterAccountType) String() string {
	return string(aat)
}

type BinanceInterval string

const (
	BINANCE_INTERVAL_1s  BinanceInterval = "1s"
	BINANCE_INTERVAL_1m  BinanceInterval = "1m"
	BINANCE_INTERVAL_3m  BinanceInterval = "3m"
	BINANCE_INTERVAL_5m  BinanceInterval = "5m"
	BINANCE_INTERVAL_15m BinanceInterval = "15m"
	BINANCE_INTERVAL_30m BinanceInterval = "30m"
	BINANCE_INTERVAL_1h  BinanceInterval = "1h"
	BINANCE_INTERVAL_2h  BinanceInterval = "2h"
	BINANCE_INTERVAL_4h  BinanceInterval = "4h"
	BINANCE_INTERVAL_6h  BinanceInterval = "6h"
	BINANCE_INTERVAL_8h  BinanceInterval = "8h"
	BINANCE_INTERVAL_12h BinanceInterval = "12h"
	BINANCE_INTERVAL_1d  BinanceInterval = "1d"
	BINANCE_INTERVAL_3d  BinanceInterval = "3d"
	BINANCE_INTERVAL_1w  BinanceInterval = "1w"
	BINANCE_INTERVAL_1M  BinanceInterval = "1M"
)

func (i BinanceInterval) String() string {
	return string(i)
}
func (i BinanceInterval) Millisecond() int64 {
	m, ok := BinanceIntervalMillisecondMap.Load(i.String())
	if !ok {
		return 0
	}
	return m
}

var BinanceIntervalMillisecondMap = NewMySyncMap[string, int64]()

func initBinanceIntervalMillisecondMap() {
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_1s.String(), 1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_1m.String(), 60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_3m.String(), 3*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_5m.String(), 5*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_15m.String(), 15*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_30m.String(), 30*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_1h.String(), 60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_2h.String(), 2*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_4h.String(), 4*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_6h.String(), 6*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_8h.String(), 8*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_12h.String(), 12*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_1d.String(), 24*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_3d.String(), 3*24*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_1w.String(), 7*24*60*60*1000)
	BinanceIntervalMillisecondMap.Store(BINANCE_INTERVAL_1M.String(), 30*24*60*60*1000)
}

type OkxInterval string

const (
	//	时间粒度，默认值1m
	//
	// 如 [1m/3m/5m/15m/30m/1H/2H/4H]
	// 香港时间开盘价k线：[6H/12H/1D/2D/3D/1W/1M/3M]
	// UTC时间开盘价k线：[/6Hutc/12Hutc/1Dutc/2Dutc/3Dutc/1Wutc/1Mutc/3Mutc]
	OKX_INTERVAL_1m     OkxInterval = "1m"
	OKX_INTERVAL_3m     OkxInterval = "3m"
	OKX_INTERVAL_5m     OkxInterval = "5m"
	OKX_INTERVAL_15m    OkxInterval = "15m"
	OKX_INTERVAL_30m    OkxInterval = "30m"
	OKX_INTERVAL_1H     OkxInterval = "1H"
	OKX_INTERVAL_2H     OkxInterval = "2H"
	OKX_INTERVAL_4H     OkxInterval = "4H"
	OKX_INTERVAL_6H     OkxInterval = "6H"
	OKX_INTERVAL_12H    OkxInterval = "12H"
	OKX_INTERVAL_1D     OkxInterval = "1D"
	OKX_INTERVAL_2D     OkxInterval = "2D"
	OKX_INTERVAL_3D     OkxInterval = "3D"
	OKX_INTERVAL_1W     OkxInterval = "1W"
	OKX_INTERVAL_1M     OkxInterval = "1M"
	OKX_INTERVAL_3M     OkxInterval = "3M"
	OKX_INTERVAL_6Hutc  OkxInterval = "6Hutc"
	OKX_INTERVAL_12Hutc OkxInterval = "12Hutc"
	OKX_INTERVAL_1Dutc  OkxInterval = "1Dutc"
	OKX_INTERVAL_2Dutc  OkxInterval = "2Dutc"
	OKX_INTERVAL_3Dutc  OkxInterval = "3Dutc"
	OKX_INTERVAL_1Wutc  OkxInterval = "1Wutc"
	OKX_INTERVAL_1Mutc  OkxInterval = "1Mutc"
	OKX_INTERVAL_3Mutc  OkxInterval = "3Mutc"
)

func (i OkxInterval) String() string {
	return string(i)
}
func (i OkxInterval) Millisecond() int64 {
	m, _ := OkxIntervalMillisecondMap.Load(i.String())
	return m
}

var OkxIntervalMillisecondMap = NewMySyncMap[string, int64]()

func initOkxIntervalMillisecondMap() {
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1m.String(), 60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_3m.String(), 3*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_5m.String(), 5*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_15m.String(), 15*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_30m.String(), 30*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1H.String(), 60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_2H.String(), 2*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_4H.String(), 4*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_6H.String(), 6*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_12H.String(), 12*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1D.String(), 24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_2D.String(), 2*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_3D.String(), 3*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1W.String(), 7*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1M.String(), 30*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_3M.String(), 3*30*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_6Hutc.String(), 6*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_12Hutc.String(), 12*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1Dutc.String(), 24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_2Dutc.String(), 2*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_3Dutc.String(), 3*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1Wutc.String(), 7*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_1Mutc.String(), 30*24*60*60*1000)
	OkxIntervalMillisecondMap.Store(OKX_INTERVAL_3Mutc.String(), 3*30*24*60*60*1000)
}

type BybitInterval string

const (
	//1 3 5 15 30 (分鐘)
	//60 120 240 360 720 (分鐘)
	//D (天)
	//W (週)
	//M (月)
	BYBIT_INTERVAL_1   BybitInterval = "1"
	BYBIT_INTERVAL_3   BybitInterval = "3"
	BYBIT_INTERVAL_5   BybitInterval = "5"
	BYBIT_INTERVAL_15  BybitInterval = "15"
	BYBIT_INTERVAL_30  BybitInterval = "30"
	BYBIT_INTERVAL_60  BybitInterval = "60"
	BYBIT_INTERVAL_120 BybitInterval = "120"
	BYBIT_INTERVAL_240 BybitInterval = "240"
	BYBIT_INTERVAL_360 BybitInterval = "360"
	BYBIT_INTERVAL_720 BybitInterval = "720"
	BYBIT_INTERVAL_D   BybitInterval = "D"
	BYBIT_INTERVAL_W   BybitInterval = "W"
	BYBIT_INTERVAL_M   BybitInterval = "M"
)

func (i BybitInterval) String() string {
	return string(i)
}
func (i BybitInterval) Millisecond() int64 {
	m, _ := BybitIntervalMillisecondMap.Load(i.String())
	return m
}

var BybitIntervalMillisecondMap = NewMySyncMap[string, int64]()

func initBybitIntervalMillisecondMap() {
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_1.String(), 60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_3.String(), 3*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_5.String(), 5*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_15.String(), 15*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_30.String(), 30*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_60.String(), 60*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_120.String(), 120*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_240.String(), 240*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_360.String(), 360*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_720.String(), 720*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_D.String(), 24*60*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_W.String(), 7*24*60*60*1000)
	BybitIntervalMillisecondMap.Store(BYBIT_INTERVAL_M.String(), 30*24*60*60*1000)
}

type GateInterval string

const (
	GATE_INTERVAL_1m  GateInterval = "1m"
	GATE_INTERVAL_3m  GateInterval = "3m"
	GATE_INTERVAL_5m  GateInterval = "5m"
	GATE_INTERVAL_15m GateInterval = "15m"
	GATE_INTERVAL_30m GateInterval = "30m"
	GATE_INTERVAL_1H  GateInterval = "1h"
	GATE_INTERVAL_2H  GateInterval = "2h"
	GATE_INTERVAL_4H  GateInterval = "4h"
	GATE_INTERVAL_6H  GateInterval = "6h"
	GATE_INTERVAL_8H  GateInterval = "8h"
	GATE_INTERVAL_12H GateInterval = "12h"
	GATE_INTERVAL_1D  GateInterval = "1d"
	GATE_INTERVAL_2D  GateInterval = "2d"
	GATE_INTERVAL_3D  GateInterval = "3d"
	GATE_INTERVAL_5D  GateInterval = "5d"
	GATE_INTERVAL_7D  GateInterval = "7d"
	GATE_INTERVAL_30D GateInterval = "30d"
)

func (i GateInterval) String() string {
	return string(i)
}

func (i GateInterval) Millisecond() int64 {
	m, _ := GateIntervalMillisecondMap.Load(i.String())
	return m
}

var GateIntervalMillisecondMap = NewMySyncMap[string, int64]()

func initGateIntervalMillisecondMap() {
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_1m.String(), 60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_3m.String(), 3*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_5m.String(), 5*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_15m.String(), 15*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_30m.String(), 30*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_1H.String(), 60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_2H.String(), 2*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_4H.String(), 4*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_6H.String(), 6*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_8H.String(), 8*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_12H.String(), 12*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_1D.String(), 24*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_2D.String(), 2*24*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_3D.String(), 3*24*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_5D.String(), 5*24*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_7D.String(), 7*24*60*60*1000)
	GateIntervalMillisecondMap.Store(GATE_INTERVAL_30D.String(), 30*24*60*60*1000)
}

type AsterInterval string

const (
	ASTER_INTERVAL_1s  AsterInterval = "1s"
	ASTER_INTERVAL_1m  AsterInterval = "1m"
	ASTER_INTERVAL_3m  AsterInterval = "3m"
	ASTER_INTERVAL_5m  AsterInterval = "5m"
	ASTER_INTERVAL_15m AsterInterval = "15m"
	ASTER_INTERVAL_30m AsterInterval = "30m"
	ASTER_INTERVAL_1h  AsterInterval = "1h"
	ASTER_INTERVAL_2h  AsterInterval = "2h"
	ASTER_INTERVAL_4h  AsterInterval = "4h"
	ASTER_INTERVAL_6h  AsterInterval = "6h"
	ASTER_INTERVAL_8h  AsterInterval = "8h"
	ASTER_INTERVAL_12h AsterInterval = "12h"
	ASTER_INTERVAL_1d  AsterInterval = "1d"
	ASTER_INTERVAL_3d  AsterInterval = "3d"
	ASTER_INTERVAL_1w  AsterInterval = "1w"
	ASTER_INTERVAL_1M  AsterInterval = "1M"
)

func (i AsterInterval) String() string {
	return string(i)
}
func (i AsterInterval) Millisecond() int64 {
	m, ok := AsterIntervalMillisecondMap.Load(i.String())
	if !ok {
		return 0
	}
	return m
}

var AsterIntervalMillisecondMap = NewMySyncMap[string, int64]()

func initAsterIntervalMillisecondMap() {
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_1s.String(), 1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_1m.String(), 60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_3m.String(), 3*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_5m.String(), 5*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_15m.String(), 15*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_30m.String(), 30*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_1h.String(), 60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_2h.String(), 2*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_4h.String(), 4*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_6h.String(), 6*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_8h.String(), 8*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_12h.String(), 12*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_1d.String(), 24*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_3d.String(), 3*24*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_1w.String(), 7*24*60*60*1000)
	AsterIntervalMillisecondMap.Store(ASTER_INTERVAL_1M.String(), 30*24*60*60*1000)
}

func init() {
	initBinanceIntervalMillisecondMap()
	initOkxIntervalMillisecondMap()
	initBybitIntervalMillisecondMap()
	initGateIntervalMillisecondMap()
	initAsterIntervalMillisecondMap()
}
