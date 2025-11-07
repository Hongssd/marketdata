package marketdata

import (
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	utils "github.com/emirpasic/gods/utils"
)

// 定义比较函数，用于比较订单的价格，假设订单的价格是float64类型
func rbtCompareBidPrice(a, b interface{}) int {
	return utils.Float64Comparator(b.(float64), a.(float64))
}

func rbtCompareAskPrice(a, b interface{}) int {
	return utils.Float64Comparator(a.(float64), b.(float64))
}

type OrderBookRBTree struct {
	Bids   *rbt.Tree
	Asks   *rbt.Tree
	bidsMu *sync.RWMutex
	asksMu *sync.RWMutex
}

func (ob *OrderBookRBTree) PutBidLevels(prices, quantities []float64) {
	if ob.Bids == nil {
		return
	}
	ob.bidsMu.Lock()
	defer ob.bidsMu.Unlock()

	for i := 0; i < len(prices); i++ {
		if quantities[i] == 0 {
			ob.Bids.Remove(prices[i])
			continue
		}
		ob.Bids.Put(prices[i], &Order{prices[i], quantities[i]})
	}
}

func (ob *OrderBookRBTree) PutAskLevels(prices, quantities []float64) {
	if ob.Asks == nil {
		return
	}
	ob.asksMu.Lock()
	defer ob.asksMu.Unlock()

	for i := 0; i < len(prices); i++ {
		if quantities[i] == 0 {
			ob.Asks.Remove(prices[i])
			continue
		}
		ob.Asks.Put(prices[i], &Order{prices[i], quantities[i]})
	}
}

func (ob *OrderBookRBTree) ClearAll() {
	ob.bidsMu.Lock()
	defer ob.bidsMu.Unlock()
	ob.asksMu.Lock()
	defer ob.asksMu.Unlock()
	ob.Bids.Clear()
	ob.Asks.Clear()
}

func (ob *OrderBookRBTree) LoadToDepth(depth *Depth, level int) (*Depth, error) {

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
	ob.bidsMu.RLock()
	defer ob.bidsMu.RUnlock()
	treeBidsIt := ob.Bids.Iterator()
	for i := 0; treeBidsIt.Next() && i < level; i++ {
		bid := treeBidsIt.Value().(*Order)
		bids = append(bids, PriceLevel{Price: bid.Price, Quantity: bid.Quantity})
	}

	ob.asksMu.RLock()
	defer ob.asksMu.RUnlock()
	treeAsksIt := ob.Asks.Iterator()
	for i := 0; treeAsksIt.Next() && i < level; i++ {
		ask := treeAsksIt.Value().(*Order)
		asks = append(asks, PriceLevel{Price: ask.Price, Quantity: ask.Quantity})
	}

	newDepth.Bids = bids
	newDepth.Asks = asks
	//log.Info(len(newDepth.Bids), len(newDepth.Asks))
	return newDepth, nil
}
