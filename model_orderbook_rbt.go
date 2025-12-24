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

var (
	// rbtSmallPool 应对 90% 的场景（如 20 档以内）
	rbtSmallPool = sync.Pool{
		New: func() any {
			return &Depth{
				Bids: make([]PriceLevel, 0, SmallLevel),
				Asks: make([]PriceLevel, 0, SmallLevel),
			}
		},
	}
	// rbtMediumPool 应对偶尔出现的深度计算
	rbtMediumPool = sync.Pool{
		New: func() any {
			return &Depth{
				Bids: make([]PriceLevel, 0, MediumLevel),
				Asks: make([]PriceLevel, 0, MediumLevel),
			}
		},
	}
)

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

// ----------------------------------------------------------------------------
// 高性能读取入口
// ----------------------------------------------------------------------------

// ViewDepth 自动管理内存生命周期，上层业务只需专注逻辑，无需关心释放
func (ob *OrderBookRBTree) ViewDepth(source *Depth, level int, bizLogic func(d *Depth) error) error {
	var d *Depth
	var pool *sync.Pool

	// 1. 自动选择池子
	if level <= SmallLevel {
		pool = &rbtSmallPool
	} else if level <= MediumLevel {
		pool = &rbtMediumPool
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
		if (pool == &rbtSmallPool && cap(d.Bids) <= SmallLevel && cap(d.Asks) <= SmallLevel) ||
			(pool == &rbtMediumPool && cap(d.Bids) <= MediumLevel && cap(d.Asks) <= MediumLevel) {
			pool.Put(d)
		}
	}
	return err
}

// ----------------------------------------------------------------------------
// 接口适配方案：LoadToDepth (外部需要直接持有指针)
// ----------------------------------------------------------------------------

func (ob *OrderBookRBTree) LoadToDepth(source *Depth, level int) (*Depth, error) {
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

func (ob *OrderBookRBTree) copyMetadata(dst, src *Depth) {
	dst.UId = src.UId
	dst.PreUId = src.PreUId
	dst.Exchange = src.Exchange
	dst.AccountType = src.AccountType
	dst.Symbol = src.Symbol
	dst.Timestamp = src.Timestamp
}

func (ob *OrderBookRBTree) fillData(d *Depth, level int) {
	// 在锁保护下创建迭代器并遍历
	ob.bidsMu.RLock()
	treeBidsIt := ob.Bids.Iterator()
	for i := 0; treeBidsIt.Next() && i < level; i++ {
		bid := treeBidsIt.Value().(*Order)
		d.Bids = append(d.Bids, PriceLevel{Price: bid.Price, Quantity: bid.Quantity})
	}
	ob.bidsMu.RUnlock()

	ob.asksMu.RLock()
	treeAsksIt := ob.Asks.Iterator()
	for i := 0; treeAsksIt.Next() && i < level; i++ {
		ask := treeAsksIt.Value().(*Order)
		d.Asks = append(d.Asks, PriceLevel{Price: ask.Price, Quantity: ask.Quantity})
	}
	ob.asksMu.RUnlock()
}
