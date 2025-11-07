package marketdata

import (
	"github.com/zhangyunhao116/skipmap"
)

// 定义比较函数，用于比较订单的价格，假设订单的价格是float64类型
func skmCompareBidPrice(a, b float64) bool {
	return a > b
}

func skmCompareAskPrice(a, b float64) bool {
	return a < b
}

type OrderBookSkipMap struct {
	Bids *skipmap.FuncMap[float64, float64]
	Asks *skipmap.FuncMap[float64, float64]
}

func (ob *OrderBookSkipMap) PutBidLevels(prices, quantities []float64) {
	if ob.Bids == nil {
		return
	}
	for i := 0; i < len(prices); i++ {
		if quantities[i] == 0 {
			ob.Bids.Delete(prices[i])
			continue
		}
		ob.Bids.Store(prices[i], quantities[i])
	}
}

func (ob *OrderBookSkipMap) PutAskLevels(prices, quantities []float64) {
	if ob.Asks == nil {
		return
	}

	for i := 0; i < len(prices); i++ {
		if quantities[i] == 0 {
			ob.Asks.Delete(prices[i])
			continue
		}
		ob.Asks.Store(prices[i], quantities[i])
	}
}

func (ob *OrderBookSkipMap) ClearAll() {
	ob.Bids = skipmap.NewFunc[float64, float64](skmCompareBidPrice)
	ob.Asks = skipmap.NewFunc[float64, float64](skmCompareAskPrice)
}

func (ob *OrderBookSkipMap) LoadToDepth(depth *Depth, level int) (*Depth, error) {

	newDepth := &Depth{
		UId:         depth.UId,
		PreUId:      depth.PreUId,
		Exchange:    depth.Exchange,
		AccountType: depth.AccountType,
		Symbol:      depth.Symbol,
		Timestamp:   depth.Timestamp,
	}

	var bids []PriceLevel
	var asks []PriceLevel

	// 在锁保护下创建迭代器并遍历
	i := 0
	ob.Bids.Range(func(key float64, value float64) bool {
		if i >= level {
			return false
		}
		bids = append(bids, PriceLevel{Price: key, Quantity: value})
		i++
		return true
	})

	i = 0
	ob.Asks.Range(func(key float64, value float64) bool {
		if i >= level {
			return false
		}
		asks = append(asks, PriceLevel{Price: key, Quantity: value})
		i++
		return true
	})

	newDepth.Bids = bids
	newDepth.Asks = asks

	return newDepth, nil
}
