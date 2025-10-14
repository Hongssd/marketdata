package marketdata

import (
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	utils "github.com/emirpasic/gods/utils"
)

// 定义比较函数，用于比较订单的价格，假设订单的价格是float64类型
func compareBidPrice(a, b interface{}) int {
	return utils.Float64Comparator(b.(float64), a.(float64))
}

func compareAskPrice(a, b interface{}) int {
	return utils.Float64Comparator(a.(float64), b.(float64))
}

// 定义一个结构体，用于表示订单的信息，你可以根据你的需求添加更多的字段
type Order struct {
	Price    float64 // 订单的价格
	Quantity float64 // 订单的数量
}
type OrderBook struct {
	Bids   *rbt.Tree
	Asks   *rbt.Tree
	bidsMu *sync.RWMutex
	asksMu *sync.RWMutex
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		Bids:   rbt.NewWith(compareBidPrice),
		Asks:   rbt.NewWith(compareAskPrice),
		bidsMu: &sync.RWMutex{},
		asksMu: &sync.RWMutex{},
	}
}
func (ob *OrderBook) PutBidLevels(prices, quantities []float64) {
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

func (ob *OrderBook) PutAskLevels(prices, quantities []float64) {
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

// func (ob *OrderBook) PutBid(price, quantity float64) {
// 	if ob.Bids == nil {
// 		return
// 	}
// 	ob.bidsMu.Lock()
// 	defer ob.bidsMu.Unlock()
// 	ob.Bids.Put(price, &Order{price, quantity})
// }
// func (ob *OrderBook) PutAsk(price, quantity float64) {
// 	if ob.Asks == nil {
// 		return
// 	}
// 	ob.asksMu.Lock()
// 	defer ob.asksMu.Unlock()
// 	ob.Asks.Put(price, &Order{price, quantity})
// }
// func (ob *OrderBook) RemoveBid(price float64) {
// 	if ob.Bids == nil {
// 		return
// 	}
// 	ob.bidsMu.Lock()
// 	defer ob.bidsMu.Unlock()
// 	ob.Bids.Remove(price)
// }
// func (ob *OrderBook) RemoveAsk(price float64) {
// 	if ob.Asks == nil {
// 		return
// 	}
// 	ob.asksMu.Lock()
// 	defer ob.asksMu.Unlock()
// 	ob.Asks.Remove(price)
// }

func (ob *OrderBook) ClearAll() {
	ob.bidsMu.Lock()
	defer ob.bidsMu.Unlock()
	ob.asksMu.Lock()
	defer ob.asksMu.Unlock()
	ob.Bids.Clear()
	ob.Asks.Clear()
}

func (ob *OrderBook) LoadToDepth(depth *Depth, level int) (*Depth, error) {
	// if level > ob.Bids.Size() || level > ob.Asks.Size() {
	// 	//err := fmt.Errorf("[%s][%s][%s]level %d is larger than orderbook size", depth.Exchange, depth.AccountType, depth.Symbol, level)
	// 	//return nil, err
	// 	//log.Infof("[%s][%s][%s]level %d is larger than orderbook size", depth.Exchange, depth.AccountType, depth.Symbol, level)
	// }

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
	treeBidsIt := ob.Bids.Iterator()
	for i := 0; treeBidsIt.Next() && i < level; i++ {
		bid := treeBidsIt.Value().(*Order)
		bids = append(bids, PriceLevel{Price: bid.Price, Quantity: bid.Quantity})
	}
	ob.bidsMu.RUnlock()

	ob.asksMu.RLock()
	treeAsksIt := ob.Asks.Iterator()
	for i := 0; treeAsksIt.Next() && i < level; i++ {
		ask := treeAsksIt.Value().(*Order)
		asks = append(asks, PriceLevel{Price: ask.Price, Quantity: ask.Quantity})
	}
	ob.asksMu.RUnlock()

	newDepth.Bids = bids
	newDepth.Asks = asks
	//log.Info(len(newDepth.Bids), len(newDepth.Asks))
	return newDepth, nil
}
