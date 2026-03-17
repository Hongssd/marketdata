package marketdata

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/myxcoinapi"
	"github.com/shopspring/decimal"
)

type XcoinOrderBook struct {
	parent                    *XcoinMarketData
	perConnSubNum             int64
	perSubMaxLen              int
	wsDepthIntervalType       myxcoinapi.WsDepthIntervalType
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	Exchange                  Exchange
	OrderBookRBTreeMap        *MySyncMap[string, OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientListMap           *MySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]
	WsClientMap               *MySyncMap[string, *myxcoinapi.PublicWsStreamClient]
	SubMap                    *MySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsDepth]]
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]
	ReSubMuMap                *MySyncMap[string, *sync.Mutex]
	ReSubWsClientMap          *MySyncMap[*myxcoinapi.PublicWsStreamClient, *sync.Mutex]
}

func (xm *XcoinMarketData) newXcoinOrderBook(config XcoinOrderBookConfig) *XcoinOrderBook {
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 10
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 50
	}
	x := &XcoinOrderBook{
		parent:                    xm,
		perConnSubNum:             config.PerConnSubNum,
		perSubMaxLen:              config.PerSubMaxLen,
		wsDepthIntervalType:       config.WsDepthIntervalType,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		Exchange:                  XCOIN,
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientListMap:           GetPointer(NewMySyncMap[*myxcoinapi.PublicWsStreamClient, *int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *myxcoinapi.PublicWsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *myxcoinapi.Subscription[myxcoinapi.WsDepth]]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
		ReSubMuMap:                GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		ReSubWsClientMap:          GetPointer(NewMySyncMap[*myxcoinapi.PublicWsStreamClient, *sync.Mutex]()),
	}

	return x
}

func (x *XcoinOrderBook) GetDepth(symbol string, level int, timeoutMilli int64) (*Depth, error) {
	depth, ok := x.OrderBookMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s depth not found", symbol)
		return nil, err
	}
	orderBook, ok := x.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
		return nil, err
	}
	newDepth, err := orderBook.LoadToDepth(depth, level)
	if err != nil {
		return nil, err
	}
	//如果超时限制大于0 判断深度是否超时
	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
		err := fmt.Errorf("symbol:%s depth timeout", symbol)
		return newDepth, err
	}
	return newDepth, nil
}

// 封装好的获取深度方法(高性能视图模式)
func (x *XcoinOrderBook) ViewDepth(symbol string, level int, timeoutMilli int64, bizLogic func(*Depth) error) error {
	depth, ok := x.OrderBookMap.Load(symbol)
	if !ok {
		return fmt.Errorf("symbol:%s depth not found", symbol)
	}
	orderBook, ok := x.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
		return err
	}

	return orderBook.ViewDepth(depth, level, func(d *Depth) error {
		//如果超时限制大于0 判断深度是否超时
		if timeoutMilli > 0 && time.Now().UnixMilli()-d.Timestamp > timeoutMilli {
			return fmt.Errorf("symbol:%s depth timeout", symbol)
		}
		return bizLogic(d)
	})
}

func (x *XcoinOrderBook) GetCurrentOrNewWsClient() (*myxcoinapi.PublicWsStreamClient, error) {
	return x.parent.GetPublicCurrentOrNewWsClient(x.perConnSubNum, x.WsClientListMap)
}
func (x *XcoinOrderBook) SubscribeOrderBook(businessType XcoinBusinessType, symbol string) error {
	return x.SubscribeOrderBookWithCallBack(businessType, symbol, nil)
}
func (x *XcoinOrderBook) SubscribeOrderBooks(businessType XcoinBusinessType, symbols []string) error {
	return x.SubscribeOrderBooksWithCallBack(businessType, symbols, nil)
}
func (x *XcoinOrderBook) SubscribeOrderBookWithCallBack(businessType XcoinBusinessType, symbol string, callback func(depth *Depth, err error)) error {
	return x.SubscribeOrderBooksWithCallBack(businessType, []string{symbol}, callback)
}
func (x *XcoinOrderBook) SubscribeOrderBooksWithCallBack(businessType XcoinBusinessType, symbols []string, callback func(depth *Depth, err error)) error {
	return x.SubscribeOrderBooksWithCallBackAndZeroCopy(businessType, symbols, callback, false)
}
func (x *XcoinOrderBook) SubscribeOrderBooksWithCallBackAndZeroCopy(businessType XcoinBusinessType, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	log.Infof("Xcoin开始订阅增量OrderBook深度，交易对数:%d, 总订阅数:%d", len(symbols), len(symbols))
	LEN := x.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := x.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = x.subscribeXcoinDepthMultipleWithZeroCopy(client, businessType, tempSymbols, callback, isZeroCopy)
			if err != nil {
				return err
			}
			currentCount, ok := x.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("OrderBook深度分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := x.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = x.subscribeXcoinDepthMultipleWithZeroCopy(client, businessType, symbols, callback, isZeroCopy)
		if err != nil {
			return err
		}
	}
	return nil
}

func (x *XcoinOrderBook) DepthContractSizeToQuantity(depth *Depth, contractSize float64) *Depth {
	for _, bid := range depth.Bids {
		bid.Quantity = decimal.NewFromFloat(bid.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
	}
	for _, ask := range depth.Asks {
		ask.Quantity = decimal.NewFromFloat(ask.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
	}
	return depth
}
func (x *XcoinOrderBook) subscribeXcoinDepth(xcoinWsClient *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbol string, callback func(depth *Depth, err error)) error {
	return x.subscribeXcoinDepthMultipleWithZeroCopy(xcoinWsClient, businessType, []string{symbol}, callback, false)
}

// 订阅Xcoin深度
func (x *XcoinOrderBook) subscribeXcoinDepthMultipleWithZeroCopy(xcoinWsClient *myxcoinapi.PublicWsStreamClient, businessType XcoinBusinessType, symbols []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	// 1) 预先准备重订阅所需的 ws 级锁，避免并发重入导致状态错乱
	if _, ok := x.ReSubWsClientMap.Load(xcoinWsClient); !ok {
		x.ReSubWsClientMap.Store(xcoinWsClient, &sync.Mutex{})
	}

	// 2) 一次性发起多 symbol 深度订阅
	xcoinSub, err := xcoinWsClient.SubscribeDepthMulti(businessType.String(), x.wsDepthIntervalType, symbols...)
	if err != nil {
		log.Error(err)
		return err
	}
	// 3) 记录订阅关系与 symbol 级互斥锁，供后续处理与重订阅使用
	for _, symbol := range symbols {
		x.WsClientMap.Store(symbol, xcoinWsClient)
		x.SubMap.Store(symbol, xcoinSub)
		x.CallBackMap.Store(symbol, callback)
		if _, ok := x.ReSubMuMap.Load(symbol); !ok {
			x.ReSubMuMap.Store(symbol, &sync.Mutex{})
		}
	}

	type cachedEvent struct {
		pre  int64
		last int64
		ev   *myxcoinapi.WsDepth
	}

	// symbol -> preUpdateId -> event，初始化前先缓存增量事件
	cacheMap := NewMySyncMap[string, *MySyncMap[int64, *myxcoinapi.WsDepth]]()

	parseInt64 := func(v string) (int64, error) {
		return strconv.ParseInt(v, 10, 64)
	}

	parseEventIDs := func(ev *myxcoinapi.WsDepth) (int64, int64, error) {
		pre, err := parseInt64(ev.PreUpdateId)
		if err != nil {
			return 0, 0, err
		}
		last, err := parseInt64(ev.LastUpdateId)
		if err != nil {
			return 0, 0, err
		}
		return pre, last, nil
	}

	resetSymbolState := func(symbol string) {
		x.OrderBookReadyUpdateIdMap.Delete(symbol)
		x.OrderBookMap.Delete(symbol)
		x.OrderBookLastUpdateIdMap.Delete(symbol)
		x.OrderBookRBTreeMap.Delete(symbol)
	}

	cacheEvent := func(symbol string, ev *myxcoinapi.WsDepth) {
		pre, _, err := parseEventIDs(ev)
		if err != nil {
			return
		}
		sm, ok := cacheMap.Load(symbol)
		if !ok {
			newMap := NewMySyncMap[int64, *myxcoinapi.WsDepth]()
			sm = &newMap
			cacheMap.Store(symbol, sm)
		}
		// 同一个 preUpdateId 后收到覆盖先收到
		sm.Store(pre, ev)
	}

	initFromSnapshotAndReplay := func(symbol string) error {
		// 4) 拉取 REST 快照，构建本地基线
		res, err := xcoin.NewRestClient("", "").PublicRestClient().
			NewPublicRestMarketDepth().
			BusinessType(businessType.String()).
			Symbol(symbol).
			Limit("1000").
			Do()
		if err != nil {
			return err
		}

		snapshot := res.Data
		snapshotLastID, err := parseInt64(snapshot.LastUpdateId)
		if err != nil {
			return err
		}

		snapshotAsEvent := &myxcoinapi.WsDepth{
			Symbol:       symbol,
			Asks:         snapshot.Asks,
			Bids:         snapshot.Bids,
			PreUpdateId:  snapshot.LastUpdateId,
			LastUpdateId: snapshot.LastUpdateId,
		}
		if err = x.initXcoinDepthOrderBook(businessType, *snapshotAsEvent); err != nil {
			return err
		}

		events := make([]cachedEvent, 0)
		if sm, ok := cacheMap.Load(symbol); ok {
			sm.Range(func(_ int64, ev *myxcoinapi.WsDepth) bool {
				pre, last, e := parseEventIDs(ev)
				if e == nil {
					events = append(events, cachedEvent{pre: pre, last: last, ev: ev})
				}
				return true
			})
		}

		sort.Slice(events, func(i, j int) bool {
			if events[i].pre == events[j].pre {
				return events[i].last < events[j].last
			}
			return events[i].pre < events[j].pre
		})

		targetPre := snapshotLastID + 1
		start := -1
		for i, ce := range events {
			// 丢弃 preUpdateId < snapshot.lastUpdateId+1 的缓存
			if ce.pre >= targetPre {
				start = i
				break
			}
		}

		currentLast := snapshotLastID
		if start >= 0 {
			// 5) 按序回放缓存事件，严格要求 preUpdateId 连续
			for i := start; i < len(events); i++ {
				ce := events[i]
				expectedPre := currentLast + 1
				if ce.pre != expectedPre {
					return fmt.Errorf("%s replay gap, preUpdateId=%d expected=%d", symbol, ce.pre, expectedPre)
				}
				if err = x.saveXcoinDepthOrderBook(businessType, *ce.ev); err != nil {
					return err
				}
				currentLast = ce.last
			}
		}
		return nil
	}

	reSubThis := func(symbol string) {
		// 6) 重订阅流程：串行退订 -> 清理本地状态 -> 重新订阅该 symbol
		if mu, ok := x.ReSubWsClientMap.Load(xcoinWsClient); ok {
			mu.Lock()
			defer mu.Unlock()
		} else {
			return
		}
		if mu, ok := x.ReSubMuMap.Load(symbol); ok {
			if mu.TryLock() {
				defer mu.Unlock()
			} else {
				return
			}
		} else {
			return
		}

		err := xcoinWsClient.UnsubscribeDepthMulti(businessType.String(), x.wsDepthIntervalType, symbol)
		for err != nil && strings.Contains(err.Error(), "websocket is busy") {
			time.Sleep(1000 * time.Millisecond)
			err = xcoinWsClient.UnsubscribeDepthMulti(businessType.String(), x.wsDepthIntervalType, symbol)
		}
		if err != nil {
			log.Error(err)
			return
		}

		x.WsClientMap.Delete(symbol)
		x.SubMap.Delete(symbol)
		x.CallBackMap.Delete(symbol)
		if count, ok := x.WsClientListMap.Load(xcoinWsClient); ok {
			atomic.AddInt64(count, -1)
		}
		resetSymbolState(symbol)

		hasSub := false
		x.SubMap.Range(func(_ string, v *myxcoinapi.Subscription[myxcoinapi.WsDepth]) bool {
			if v == xcoinSub {
				hasSub = true
				return false
			}
			return true
		})
		if !hasSub {
			xcoinSub.CloseChan() <- struct{}{}
		}

		err = x.subscribeXcoinDepthMultipleWithZeroCopy(xcoinWsClient, businessType, []string{symbol}, callback, isZeroCopy)
		if err != nil {
			log.Error(err)
		}
	}

	go func() {
		// 7) 消费 ws 事件：错误、增量数据、关闭信号
		for {
			select {
			case err := <-xcoinSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case result := <-xcoinSub.ResultChan():
				symbol := result.Symbol
				preID, lastID, err := parseEventIDs(&result)
				if err != nil {
					log.Error(err)
					go reSubThis(symbol)
					continue
				}

				_, readyErr := x.checkXcoinDepthIsReady(symbol)
				if readyErr != nil {
					// 本地副本未就绪：先缓存增量，再异步执行快照初始化 + 回放
					cacheEvent(symbol, &result)
					if mu, ok := x.ReSubMuMap.Load(symbol); ok && mu.TryLock() {
						go func(sym string, m *sync.Mutex) {
							defer m.Unlock()
							if e := initFromSnapshotAndReplay(sym); e != nil {
								log.Error(e)
								go reSubThis(sym)
							}
						}(symbol, mu)
					}
					continue
				}

				lastLocal, ok := x.OrderBookLastUpdateIdMap.Load(symbol)
				if !ok {
					go reSubThis(symbol)
					continue
				}
				// 连续性校验：当前 preUpdateId 必须等于上一条 lastUpdateId+1
				if preID != lastLocal+1 {
					go reSubThis(symbol)
					continue
				}

				// 本地账本增量落盘
				if err = x.saveXcoinDepthOrderBook(businessType, result); err != nil {
					log.Error(err)
					go reSubThis(symbol)
					continue
				}

				if callback == nil || x.callBackDepthLevel == 0 {
					continue
				}

				if isZeroCopy {
					// 8) 零拷贝回调：直接在视图中读取并回调，减少对象分配
					err = x.ViewDepth(symbol, int(x.callBackDepthLevel), x.callBackDepthTimeoutMilli, func(d *Depth) error {
						d.UId = lastID
						d.PreUId = preID
						callback(d, nil)
						return nil
					})
					if err != nil {
						callback(nil, err)
					}
				} else {
					// 非零拷贝回调：生成独立 Depth 副本再回调
					depth, err := x.GetDepth(symbol, int(x.callBackDepthLevel), x.callBackDepthTimeoutMilli)
					if err != nil {
						callback(nil, err)
						continue
					}
					depth.UId = lastID
					depth.PreUId = preID
					callback(depth, nil)
				}
			case <-xcoinSub.CloseChan():
				return
			}
		}
	}()

	// 9) 更新该 ws 连接当前承载的订阅计数
	currentCount := int64(len(symbols))
	count, ok := x.WsClientListMap.Load(xcoinWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		x.WsClientListMap.Store(xcoinWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (x *XcoinOrderBook) checkXcoinDepthIsReady(symbol string) (int64, error) {
	readyID, ok := x.OrderBookReadyUpdateIdMap.Load(symbol)
	if !ok {
		return 0, fmt.Errorf("%s depth not ready", symbol)
	}
	return readyID, nil
}

func (x *XcoinOrderBook) initXcoinDepthOrderBook(businessType XcoinBusinessType, result myxcoinapi.WsDepth) error {
	x.initAndClearXcoinDepthOrderBook(result)
	if err := x.saveXcoinDepthOrderBook(businessType, result); err != nil {
		return err
	}
	return nil
}

func (x *XcoinOrderBook) initAndClearXcoinDepthOrderBook(result myxcoinapi.WsDepth) {
	Symbol := result.Symbol
	orderBook, ok := x.OrderBookRBTreeMap.Load(Symbol)
	if !ok || orderBook == nil {
		orderBook = NewOrderBook()
		x.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	orderBook.ClearAll()
	x.OrderBookReadyUpdateIdMap.Delete(Symbol)
	x.OrderBookMap.Delete(Symbol)
	x.OrderBookLastUpdateIdMap.Delete(Symbol)
}

// 将Depth保存至OrderBook
func (x *XcoinOrderBook) saveXcoinDepthOrderBook(businessType XcoinBusinessType, result myxcoinapi.WsDepth) error {
	Symbol := result.Symbol
	lastID, err := strconv.ParseInt(result.LastUpdateId, 10, 64)
	if err != nil {
		return err
	}
	preID, err := strconv.ParseInt(result.PreUpdateId, 10, 64)
	if err != nil {
		// 快照初始化时可能没有 preUpdateId，回退到 lastID
		preID = lastID
	}

	lastUpdateId, ok := x.OrderBookLastUpdateIdMap.Load(Symbol)
	if ok {
		if lastID > lastUpdateId {
			//正常推送
		} else if lastID == lastUpdateId {
			//无更新
			return nil
		} else if lastID < lastUpdateId {
			//序列重置
			log.Warnf("%s lastUpdateId reset %d to %d", Symbol, lastUpdateId, lastID)
		}
	} else {
		lastUpdateId = preID
	}
	x.OrderBookLastUpdateIdMap.Store(Symbol, lastID)
	orderBook, ok := x.OrderBookRBTreeMap.Load(Symbol)
	if !ok || orderBook == nil {
		orderBook = NewOrderBook()
		x.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}
	bidPrices := make([]float64, 0, len(result.Bids))
	bidQuantities := make([]float64, 0, len(result.Bids))
	askPrices := make([]float64, 0, len(result.Asks))
	askQuantities := make([]float64, 0, len(result.Asks))

	for _, bid := range result.Bids {
		p, err := strconv.ParseFloat(bid.Price, 64)
		if err != nil {
			return err
		}
		q, err := strconv.ParseFloat(bid.Quantity, 64)
		if err != nil {
			return err
		}
		bidPrices = append(bidPrices, p)
		bidQuantities = append(bidQuantities, q)
	}
	for _, ask := range result.Asks {
		p, err := strconv.ParseFloat(ask.Price, 64)
		if err != nil {
			return err
		}
		q, err := strconv.ParseFloat(ask.Quantity, 64)
		if err != nil {
			return err
		}
		askPrices = append(askPrices, p)
		askQuantities = append(askQuantities, q)
	}

	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)

	now := time.Now().UnixMilli()
	targetTs := now + x.parent.GetServerTimeDelta()
	if result.Ts > 0 {
		targetTs = result.Ts + x.parent.GetServerTimeDelta()
	}
	if targetTs > now {
		targetTs = now
	}
	depth := &Depth{
		UId:         lastID,
		PreUId:      lastUpdateId,
		AccountType: businessType.String(),
		Exchange:    x.Exchange.String(),
		Symbol:      Symbol,
		Timestamp:   targetTs,
	}
	x.OrderBookMap.Store(Symbol, depth)
	x.OrderBookReadyUpdateIdMap.Store(Symbol, lastID)
	return nil
}

func (x *XcoinOrderBook) Close() {
	x.WsClientListMap.Range(func(k *myxcoinapi.PublicWsStreamClient, v *int64) bool {
		err := k.Close()
		if err != nil {
			log.Error(err)
		}
		return true
	})
	x.OrderBookRBTreeMap.Clear()
	x.OrderBookReadyUpdateIdMap.Clear()
	x.OrderBookMap.Clear()
	x.OrderBookLastUpdateIdMap.Clear()
	x.WsClientListMap.Clear()
	x.WsClientMap.Clear()
	x.SubMap.Clear()
	x.CallBackMap.Clear()
	x.ReSubMuMap.Clear()
	x.ReSubWsClientMap.Clear()
}
