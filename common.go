package marketdata

import (
	"strconv"
	"sync"
	"time"

	"github.com/Hongssd/mygateapi"

	"github.com/Hongssd/mybybitapi"
)

type MySyncMap[K any, V any] struct {
	smap sync.Map
}

func NewMySyncMap[K any, V any]() MySyncMap[K, V] {
	return MySyncMap[K, V]{
		smap: sync.Map{},
	}
}
func (m *MySyncMap[K, V]) Load(k K) (V, bool) {
	v, ok := m.smap.Load(k)

	if ok {
		return v.(V), true
	}
	var resv V
	return resv, false
}
func (m *MySyncMap[K, V]) Store(k K, v V) {
	m.smap.Store(k, v)
}

func (m *MySyncMap[K, V]) Delete(k K) {
	m.smap.Delete(k)
}
func (m *MySyncMap[K, V]) Range(f func(k K, v V) bool) {
	m.smap.Range(func(k, v any) bool {
		return f(k.(K), v.(V))
	})
}

func (m *MySyncMap[K, V]) Length() int {
	length := 0
	m.Range(func(k K, v V) bool {
		length += 1
		return true
	})
	return length
}

func (m *MySyncMap[K, V]) Clear() {
	m.smap.Range(func(k, v any) bool {
		m.smap.Delete(k)
		return true
	})
}

func (m *MySyncMap[K, V]) MapValues(f func(k K, v V) V) *MySyncMap[K, V] {
	var res = NewMySyncMap[K, V]()
	m.Range(func(k K, v V) bool {
		res.Store(k, f(k, v))
		return true
	})
	return &res
}

func GetPointer[T any](v T) *T {
	return &v
}

func stringToFloat64(str string) float64 {
	f, _ := strconv.ParseFloat(str, 64)
	return f
}

func stringToInt64(str string) int64 {
	i, _ := strconv.ParseInt(str, 10, 64)
	return i
}

func BinanceGetServerTimeDelta(accountType BinanceAccountType) (int64, error) {

	switch accountType {
	case BINANCE_SPOT:
		res, err := binance.NewSpotRestClient("", "").NewServerTime().Do()
		if err != nil {
			return 0, err
		}
		start := time.Now().UnixMilli()
		return start - res.ServerTime, nil
	case BINANCE_FUTURE:
		res, err := binance.NewFutureRestClient("", "").NewServerTime().Do()
		if err != nil {
			return 0, err
		}
		start := time.Now().UnixMilli()
		return start - res.ServerTime, nil
	case BINANCE_SWAP:
		res, err := binance.NewSwapRestClient("", "").NewServerTime().Do()
		if err != nil {
			return 0, err
		}
		start := time.Now().UnixMilli()
		return start - res.ServerTime, nil
	default:
		return 0, ErrorAccountType
	}
}

func OkxGetServerTimeDelta() (int64, error) {

	res, err := okx.NewRestClient("", "", "").PublicRestClient().NewPublicRestPublicTime().Do()
	if err != nil {
		return 0, err
	}
	serverTime, err := strconv.ParseInt(res.Data[0].Ts, 10, 64)
	if err != nil {
		return 0, err
	}
	start := time.Now().UnixMilli()
	return start - serverTime, nil
}

func BybitGetServerTimeDelta() (int64, error) {

	res, err := mybybitapi.NewRestClient("", "").PublicRestClient().NewMarketTime().Do()
	if err != nil {
		return 0, err
	}
	serverTimeNano, err := strconv.ParseInt(res.Result.TimeNano, 10, 64)
	if err != nil {
		return 0, err
	}
	start := time.Now().UnixMilli()
	serverTime := serverTimeNano / 1e6
	return start - serverTime, nil
}

func GateGetServerTimeDelta() (int64, error) {

	res, err := mygateapi.NewRestClient("", "").PublicRestClient().NewPublicRestSpotTime().Do()
	if err != nil {
		return 0, err
	}
	start := time.Now().UnixMilli()
	return start - res.Data.ServerTime, nil
}
