package marketdata

import (
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/zhangyunhao116/skipmap"
)

// 定义一个结构体，用于表示订单的信息，你可以根据你的需求添加更多的字段
type Order struct {
	Price    float64 // 订单的价格
	Quantity float64 // 订单的数量
}

type OrderBook interface {
	PutBidLevels(prices, quantities []float64)
	PutAskLevels(prices, quantities []float64)
	ClearAll()
	LoadToDepth(depth *Depth, level int) (*Depth, error)
}

type OrderBookType int

const (
	OBT_RBTREE OrderBookType = iota
	OBT_SKIPMAP
)

var currentOrderBookType OrderBookType = OBT_RBTREE

func SetOrderBookType(orderBookType OrderBookType) {
	currentOrderBookType = orderBookType
}

func NewOrderBook() OrderBook {
	switch currentOrderBookType {
	case OBT_RBTREE:
		return &OrderBookRBTree{
			Bids:   rbt.NewWith(rbtCompareBidPrice),
			Asks:   rbt.NewWith(rbtCompareAskPrice),
			bidsMu: &sync.RWMutex{},
			asksMu: &sync.RWMutex{},
		}
	case OBT_SKIPMAP:
		return &OrderBookSkipMap{
			Bids: skipmap.NewFunc[float64, float64](skmCompareBidPrice),
			Asks: skipmap.NewFunc[float64, float64](skmCompareAskPrice),
		}
	}
	return nil

}
