package marketdata

import (
	"sync"

	"github.com/zhangyunhao116/skipmap"
)

// 定义常见的档位常量，用于预分配内存
const (
	SmallLevel  = 20
	MediumLevel = 100
)

var (
	// smallPool 应对 90% 的场景（如 20 档以内）
	smallPool = sync.Pool{
		New: func() any {
			return &Depth{
				Bids: make([]PriceLevel, 0, SmallLevel),
				Asks: make([]PriceLevel, 0, SmallLevel),
			}
		},
	}
	// mediumPool 应对偶尔出现的深度计算
	mediumPool = sync.Pool{
		New: func() any {
			return &Depth{
				Bids: make([]PriceLevel, 0, MediumLevel),
				Asks: make([]PriceLevel, 0, MediumLevel),
			}
		},
	}
)

type OrderBookSkipMap struct {
	mu   sync.RWMutex
	Bids *skipmap.FuncMap[float64, float64]
	Asks *skipmap.FuncMap[float64, float64]
}

// NewOrderBookSkipMap 构造函数
func NewOrderBookSkipMap() *OrderBookSkipMap {
	return &OrderBookSkipMap{
		Bids: skipmap.NewFunc[float64, float64](func(a, b float64) bool { return a > b }),
		Asks: skipmap.NewFunc[float64, float64](func(a, b float64) bool { return a < b }),
	}
}

func (ob *OrderBookSkipMap) PutBidLevels(prices, quantities []float64) {
	ob.mu.RLock()
	bids := ob.Bids
	ob.mu.RUnlock()

	if bids == nil {
		return
	}
	for i := 0; i < len(prices); i++ {
		if quantities[i] == 0 {
			bids.Delete(prices[i])
		} else {
			bids.Store(prices[i], quantities[i])
		}
	}
}

func (ob *OrderBookSkipMap) PutAskLevels(prices, quantities []float64) {
	ob.mu.RLock()
	asks := ob.Asks
	ob.mu.RUnlock()

	if asks == nil {
		return
	}
	for i := 0; i < len(prices); i++ {
		if quantities[i] == 0 {
			asks.Delete(prices[i])
		} else {
			asks.Store(prices[i], quantities[i])
		}
	}
}

func (ob *OrderBookSkipMap) ClearAll() {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.Bids = skipmap.NewFunc[float64, float64](func(a, b float64) bool { return a > b })
	ob.Asks = skipmap.NewFunc[float64, float64](func(a, b float64) bool { return a < b })
}

// ----------------------------------------------------------------------------
// 高性能读取入口：推荐策略和 Redis 存储使用此方法
// ----------------------------------------------------------------------------

// ViewDepth 自动管理内存生命周期，上层业务只需专注逻辑，无需关心释放
func (ob *OrderBookSkipMap) ViewDepth(source *Depth, level int, bizLogic func(d *Depth) error) error {
	var d *Depth
	var pool *sync.Pool

	// 1. 自动选择池子
	if level <= SmallLevel {
		pool = &smallPool
	} else if level <= MediumLevel {
		pool = &mediumPool
	}

	if pool != nil {
		d = pool.Get().(*Depth)
		// 确保切片长度重置
		d.Bids = d.Bids[:0]
		d.Asks = d.Asks[:0]
	} else {
		// 超过 100 档，直接分配（非池化路径）
		d = &Depth{
			Bids: make([]PriceLevel, 0, level),
			Asks: make([]PriceLevel, 0, level),
		}
	}

	// 2. 拷贝元数据
	ob.copyMetadata(d, source)

	// 3. 填充盘口数据
	ob.fillData(d, level)

	// 4. 执行业务逻辑
	err := bizLogic(d)

	// 5. 回收处理（带防污染检查）
	if pool != nil {
		// 如果因为 append 导致容量溢出，则不还回池子，防止污染
		if (pool == &smallPool && cap(d.Bids) <= SmallLevel && cap(d.Asks) <= SmallLevel) ||
			(pool == &mediumPool && cap(d.Bids) <= MediumLevel && cap(d.Asks) <= MediumLevel) {
			pool.Put(d)
		}
	}
	return err
}

// ----------------------------------------------------------------------------
// 接口适配方案：LoadToDepth (外部需要直接持有指针)
// ----------------------------------------------------------------------------

func (ob *OrderBookSkipMap) LoadToDepth(source *Depth, level int) (*Depth, error) {
	// 由于此方法返回指针且外部可能不调用回收函数，
	// 为了系统安全，此处不使用池化，直接新分配内存。
	newDepth := &Depth{
		Bids: make([]PriceLevel, 0, level),
		Asks: make([]PriceLevel, 0, level),
	}
	ob.copyMetadata(newDepth, source)
	ob.fillData(newDepth, level)
	return newDepth, nil
}

// ----------------------------------------------------------------------------
// 内部私有辅助函数
// ----------------------------------------------------------------------------

func (ob *OrderBookSkipMap) copyMetadata(dst, src *Depth) {
	dst.UId = src.UId
	dst.PreUId = src.PreUId
	dst.Exchange = src.Exchange
	dst.AccountType = src.AccountType
	dst.Symbol = src.Symbol
	dst.Timestamp = src.Timestamp
}

func (ob *OrderBookSkipMap) fillData(d *Depth, level int) {
	ob.mu.RLock()
	bids := ob.Bids
	asks := ob.Asks
	ob.mu.RUnlock()

	if bids != nil {
		i := 0
		bids.Range(func(key float64, value float64) bool {
			if i >= level {
				return false
			}
			d.Bids = append(d.Bids, PriceLevel{Price: key, Quantity: value})
			i++
			return true
		})
	}

	if asks != nil {
		i := 0
		asks.Range(func(key float64, value float64) bool {
			if i >= level {
				return false
			}
			d.Asks = append(d.Asks, PriceLevel{Price: key, Quantity: value})
			i++
			return true
		})
	}
}
