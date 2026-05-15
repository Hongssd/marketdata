package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mypolymarketapi"
)

type PolymarketOrderBookConfig struct {
	Level                     int   // 订阅 level（1/2/3）
	PerConnSubNum             int64 // 每条链接承载的资产数量上限
	PerSubMaxLen              int   // 单次订阅最大资产数量
	SubChannelBufferSize      int   // 订阅通道缓冲
	CallBackDepthLevel        int64 // 回调深度档位
	CallBackDepthTimeoutMilli int64 // 回调超时毫秒
}

var PolymarketOrderBookConfigDefault = PolymarketOrderBookConfig{
	Level:                     2,
	PerConnSubNum:             200,
	PerSubMaxLen:              100,
	SubChannelBufferSize:      1024,
	CallBackDepthLevel:        20,
	CallBackDepthTimeoutMilli: 5000,
}

type polymarketPendingPriceChange struct {
	mu      sync.Mutex
	changes []mypolymarketapi.WsMarketPriceChange
}

type PolymarketOrderBook struct {
	perConnSubNum             int64
	perSubMaxLen              int
	subChannelBufferSize      int
	callBackDepthLevel        int64
	callBackDepthTimeoutMilli int64
	level                     int
	Exchange                  Exchange

	subscriberSeq atomic.Int64

	OrderBookRBTreeMap        *MySyncMap[string, OrderBook]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	PendingPriceChangeMap     *MySyncMap[string, *polymarketPendingPriceChange]

	WsClientListMap *MySyncMap[*mypolymarketapi.MarketWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *mypolymarketapi.MarketWsStreamClient]
	SubscriberIDMap *MySyncMap[string, string]
}

func NewPolymarketOrderBook(config PolymarketOrderBookConfig) *PolymarketOrderBook {
	if config.Level == 0 {
		config.Level = PolymarketOrderBookConfigDefault.Level
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = PolymarketOrderBookConfigDefault.PerConnSubNum
	}
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = PolymarketOrderBookConfigDefault.PerSubMaxLen
	}
	if config.SubChannelBufferSize == 0 {
		config.SubChannelBufferSize = PolymarketOrderBookConfigDefault.SubChannelBufferSize
	}
	if config.CallBackDepthLevel == 0 {
		config.CallBackDepthLevel = PolymarketOrderBookConfigDefault.CallBackDepthLevel
	}
	if config.CallBackDepthTimeoutMilli == 0 {
		config.CallBackDepthTimeoutMilli = PolymarketOrderBookConfigDefault.CallBackDepthTimeoutMilli
	}

	return &PolymarketOrderBook{
		perConnSubNum:             config.PerConnSubNum,
		perSubMaxLen:              config.PerSubMaxLen,
		subChannelBufferSize:      config.SubChannelBufferSize,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		level:                     config.Level,
		Exchange:                  POLYMARKET,
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, OrderBook]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		PendingPriceChangeMap:     GetPointer(NewMySyncMap[string, *polymarketPendingPriceChange]()),
		WsClientListMap:           GetPointer(NewMySyncMap[*mypolymarketapi.MarketWsStreamClient, *int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mypolymarketapi.MarketWsStreamClient]()),
		SubscriberIDMap:           GetPointer(NewMySyncMap[string, string]()),
	}
}

func (p *PolymarketOrderBook) GetCurrentOrNewWsClient() (*mypolymarketapi.MarketWsStreamClient, error) {
	var wsClient *mypolymarketapi.MarketWsStreamClient
	p.WsClientListMap.Range(func(k *mypolymarketapi.MarketWsStreamClient, v *int64) bool {
		if *v < p.perConnSubNum {
			wsClient = k
			return false
		}
		return true
	})
	if wsClient != nil {
		return wsClient, nil
	}

	pm := mypolymarketapi.MyPolymarket{}
	wsClient = pm.NewMarketWsStreamClient()
	wsClient.SetLevel(p.level)
	if err := wsClient.OpenConn(); err != nil {
		return nil, err
	}

	initCount := int64(0)
	p.WsClientListMap.Store(wsClient, &initCount)
	if p.WsClientListMap.Length() > 1 {
		log.Infof("Polymarket当前链接订阅权重已用完，建立新的Ws链接，当前链接数:%d ...", p.WsClientListMap.Length())
	} else {
		log.Info("Polymarket首次建立新的Ws链接...")
	}
	return wsClient, nil
}

func (p *PolymarketOrderBook) GetDepth(assetID string, level int, timeoutMilli int64) (*Depth, error) {
	depth, ok := p.OrderBookMap.Load(assetID)
	if !ok {
		return nil, fmt.Errorf("asset_id:%s depth not found", assetID)
	}
	orderBook, ok := p.OrderBookRBTreeMap.Load(assetID)
	if !ok {
		return nil, fmt.Errorf("asset_id:%s orderbook not found", assetID)
	}

	newDepth, err := orderBook.LoadToDepth(depth, level)
	if err != nil {
		return nil, err
	}
	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
		return newDepth, fmt.Errorf("asset_id:%s depth timeout", assetID)
	}
	return newDepth, nil
}

func (p *PolymarketOrderBook) ViewDepth(assetID string, level int, timeoutMilli int64, bizLogic func(*Depth) error) error {
	depth, ok := p.OrderBookMap.Load(assetID)
	if !ok {
		return fmt.Errorf("asset_id:%s depth not found", assetID)
	}
	orderBook, ok := p.OrderBookRBTreeMap.Load(assetID)
	if !ok {
		return fmt.Errorf("asset_id:%s orderbook not found", assetID)
	}

	return orderBook.ViewDepth(depth, level, func(d *Depth) error {
		if timeoutMilli > 0 && time.Now().UnixMilli()-d.Timestamp > timeoutMilli {
			return fmt.Errorf("asset_id:%s depth timeout", assetID)
		}
		return bizLogic(d)
	})
}

func (p *PolymarketOrderBook) SubscribeOrderBook(assetID string) error {
	return p.SubscribeOrderBookWithCallBack(assetID, nil)
}

func (p *PolymarketOrderBook) SubscribeOrderBooks(assetIDs []string) error {
	return p.SubscribeOrderBooksWithCallBack(assetIDs, nil)
}

func (p *PolymarketOrderBook) SubscribeOrderBookWithCallBack(assetID string, callback func(depth *Depth, err error)) error {
	return p.SubscribeOrderBooksWithCallBack([]string{assetID}, callback)
}

func (p *PolymarketOrderBook) SubscribeOrderBooksWithCallBack(assetIDs []string, callback func(depth *Depth, err error)) error {
	return p.SubscribeOrderBooksWithCallBackAndZeroCopy(assetIDs, callback, false)
}

func (p *PolymarketOrderBook) SubscribeOrderBooksWithCallBackAndZeroCopy(assetIDs []string, callback func(depth *Depth, err error), isZeroCopy bool) error {
	if len(assetIDs) == 0 {
		return errors.New("assetIDs is empty")
	}
	log.Infof("Polymarket开始订阅OrderBook深度，资产ID数:%d", len(assetIDs))

	subBatchSize := p.perSubMaxLen
	if subBatchSize <= 0 {
		subBatchSize = len(assetIDs)
	}

	for i := 0; i < len(assetIDs); i += subBatchSize {
		end := i + subBatchSize
		if end > len(assetIDs) {
			end = len(assetIDs)
		}
		batch := assetIDs[i:end]
		wsClient, err := p.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = p.subscribePolymarketDepthMultipleWithZeroCopy(wsClient, batch, callback, isZeroCopy)
		if err != nil {
			return err
		}
		if end < len(assetIDs) {
			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Infof("Polymarket订阅OrderBook结束，资产ID数:%d", len(assetIDs))
	return nil
}

func (p *PolymarketOrderBook) subscribePolymarketDepthMultipleWithZeroCopy(
	wsClient *mypolymarketapi.MarketWsStreamClient,
	assetIDs []string,
	callback func(depth *Depth, err error),
	isZeroCopy bool,
) error {
	subscriberID := p.nextSubscriberID()

	bookChan, err := wsClient.SubscribeOrderBook(subscriberID, assetIDs, p.subChannelBufferSize)
	if err != nil {
		return err
	}
	priceChangeChan, err := wsClient.SubscribePriceChange(subscriberID, assetIDs, p.subChannelBufferSize)
	if err != nil {
		wsClient.Unsubscribe(subscriberID)
		return err
	}

	for _, assetID := range assetIDs {
		p.WsClientMap.Store(assetID, wsClient)
		p.SubscriberIDMap.Store(assetID, subscriberID)
	}

	go p.consumeMarketStreams(bookChan, priceChangeChan, callback, isZeroCopy)

	currentCount := int64(len(assetIDs))
	count, ok := p.WsClientListMap.Load(wsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		p.WsClientListMap.Store(wsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

func (p *PolymarketOrderBook) consumeMarketStreams(
	bookChan <-chan mypolymarketapi.WsMarketOrderBook,
	priceChangeChan <-chan mypolymarketapi.WsMarketPriceChange,
	callback func(depth *Depth, err error),
	isZeroCopy bool,
) {
	for bookChan != nil || priceChangeChan != nil {
		select {
		case book, ok := <-bookChan:
			if !ok {
				bookChan = nil
				continue
			}
			if err := p.handleBookSnapshot(book); err != nil {
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
				continue
			}
			if callback != nil && p.callBackDepthLevel > 0 {
				p.emitDepthCallback(book.AssetID, callback, isZeroCopy)
			}
		case pc, ok := <-priceChangeChan:
			if !ok {
				priceChangeChan = nil
				continue
			}
			if err := p.handlePriceChange(pc); err != nil {
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
				continue
			}
			if callback != nil && p.callBackDepthLevel > 0 {
				p.emitDepthCallback(pc.AssetID, callback, isZeroCopy)
			}
		}
	}
}

func (p *PolymarketOrderBook) emitDepthCallback(assetID string, callback func(depth *Depth, err error), isZeroCopy bool) {
	if isZeroCopy {
		err := p.ViewDepth(assetID, int(p.callBackDepthLevel), p.callBackDepthTimeoutMilli, func(d *Depth) error {
			callback(d, nil)
			return nil
		})
		if err != nil {
			callback(nil, err)
		}
		return
	}

	depth, err := p.GetDepth(assetID, int(p.callBackDepthLevel), p.callBackDepthTimeoutMilli)
	if err != nil {
		callback(nil, err)
		return
	}
	callback(depth, nil)
}

func (p *PolymarketOrderBook) handleBookSnapshot(book mypolymarketapi.WsMarketOrderBook) error {
	assetID := book.AssetID
	orderBook := p.getOrCreateOrderBook(assetID)
	orderBook.ClearAll()

	bidPrices, bidQuantities, err := parseWsOrderSummary(book.Bids)
	if err != nil {
		return err
	}
	askPrices, askQuantities, err := parseWsOrderSummary(book.Asks)
	if err != nil {
		return err
	}

	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)

	ts := parsePolymarketTimestamp(book.Timestamp)
	p.updateDepthMeta(assetID, ts)
	p.OrderBookReadyUpdateIdMap.Store(assetID, p.mustGetLastUpdateID(assetID))

	// book 是全量快照，快照到达后回放在此之前暂存的增量。
	pending := p.popPendingPriceChanges(assetID)
	for _, chg := range pending {
		if err = p.applyPriceChange(chg); err != nil {
			return err
		}
	}
	return nil
}

func (p *PolymarketOrderBook) handlePriceChange(chg mypolymarketapi.WsMarketPriceChange) error {
	assetID := chg.AssetID
	if _, ok := p.OrderBookReadyUpdateIdMap.Load(assetID); !ok {
		p.appendPendingPriceChange(assetID, chg)
		return nil
	}
	return p.applyPriceChange(chg)
}

func (p *PolymarketOrderBook) applyPriceChange(chg mypolymarketapi.WsMarketPriceChange) error {
	assetID := chg.AssetID
	orderBook := p.getOrCreateOrderBook(assetID)

	price, err := strconv.ParseFloat(chg.Price, 64)
	if err != nil {
		return fmt.Errorf("asset_id:%s parse price failed: %w", assetID, err)
	}
	size, err := strconv.ParseFloat(chg.Size, 64)
	if err != nil {
		return fmt.Errorf("asset_id:%s parse size failed: %w", assetID, err)
	}

	switch strings.ToUpper(chg.Side) {
	case "BUY":
		orderBook.PutBidLevels([]float64{price}, []float64{size})
	case "SELL":
		orderBook.PutAskLevels([]float64{price}, []float64{size})
	default:
		return fmt.Errorf("asset_id:%s unknown side:%s", assetID, chg.Side)
	}

	p.updateDepthMeta(assetID, time.Now().UnixMilli())
	p.OrderBookReadyUpdateIdMap.Store(assetID, p.mustGetLastUpdateID(assetID))
	return nil
}

func (p *PolymarketOrderBook) updateDepthMeta(assetID string, ts int64) {
	now := time.Now().UnixMilli()
	if ts <= 0 || ts > now {
		ts = now
	}

	uid, preUID := p.nextUpdateID(assetID, ts)
	p.OrderBookMap.Store(assetID, &Depth{
		UId:         uid,
		PreUId:      preUID,
		AccountType: "MARKET",
		Exchange:    p.Exchange.String(),
		Symbol:      assetID,
		Timestamp:   ts,
	})
}

func (p *PolymarketOrderBook) nextUpdateID(assetID string, tsHint int64) (int64, int64) {
	lastID, _ := p.OrderBookLastUpdateIdMap.Load(assetID)
	nextID := lastID + 1
	if lastID == 0 && tsHint > 0 {
		nextID = tsHint
	} else if tsHint > nextID {
		nextID = tsHint
	}
	p.OrderBookLastUpdateIdMap.Store(assetID, nextID)
	return nextID, lastID
}

func (p *PolymarketOrderBook) mustGetLastUpdateID(assetID string) int64 {
	v, ok := p.OrderBookLastUpdateIdMap.Load(assetID)
	if !ok {
		return 0
	}
	return v
}

func (p *PolymarketOrderBook) getOrCreateOrderBook(assetID string) OrderBook {
	orderBook, ok := p.OrderBookRBTreeMap.Load(assetID)
	if ok && orderBook != nil {
		return orderBook
	}
	orderBook = NewOrderBook()
	p.OrderBookRBTreeMap.Store(assetID, orderBook)
	return orderBook
}

func (p *PolymarketOrderBook) appendPendingPriceChange(assetID string, chg mypolymarketapi.WsMarketPriceChange) {
	cache, ok := p.PendingPriceChangeMap.Load(assetID)
	if !ok {
		cache = &polymarketPendingPriceChange{}
		p.PendingPriceChangeMap.Store(assetID, cache)
	}
	cache.mu.Lock()
	cache.changes = append(cache.changes, chg)
	cache.mu.Unlock()
}

func (p *PolymarketOrderBook) popPendingPriceChanges(assetID string) []mypolymarketapi.WsMarketPriceChange {
	cache, ok := p.PendingPriceChangeMap.Load(assetID)
	if !ok || cache == nil {
		return nil
	}
	cache.mu.Lock()
	out := make([]mypolymarketapi.WsMarketPriceChange, len(cache.changes))
	copy(out, cache.changes)
	cache.changes = cache.changes[:0]
	cache.mu.Unlock()
	p.PendingPriceChangeMap.Delete(assetID)
	return out
}

func (p *PolymarketOrderBook) nextSubscriberID() string {
	return fmt.Sprintf("pm-ob-%d", p.subscriberSeq.Add(1))
}

func (p *PolymarketOrderBook) Close() {
	type noop struct{}

	wsToSubIDs := map[*mypolymarketapi.MarketWsStreamClient]map[string]noop{}
	p.WsClientMap.Range(func(assetID string, ws *mypolymarketapi.MarketWsStreamClient) bool {
		subID, ok := p.SubscriberIDMap.Load(assetID)
		if !ok || ws == nil {
			return true
		}
		subIDSet, exists := wsToSubIDs[ws]
		if !exists {
			subIDSet = map[string]noop{}
			wsToSubIDs[ws] = subIDSet
		}
		subIDSet[subID] = noop{}
		return true
	})

	for ws, subIDs := range wsToSubIDs {
		for subID := range subIDs {
			ws.Unsubscribe(subID)
		}
		if err := ws.Close(); err != nil {
			log.Error(err)
		}
	}

	p.OrderBookRBTreeMap.Clear()
	p.OrderBookMap.Clear()
	p.OrderBookReadyUpdateIdMap.Clear()
	p.OrderBookLastUpdateIdMap.Clear()
	p.PendingPriceChangeMap.Clear()
	p.WsClientListMap.Clear()
	p.WsClientMap.Clear()
	p.SubscriberIDMap.Clear()
}

func parseWsOrderSummary(items []mypolymarketapi.WsMarketOrderSummary) ([]float64, []float64, error) {
	prices := make([]float64, 0, len(items))
	quantities := make([]float64, 0, len(items))
	for _, item := range items {
		price, err := strconv.ParseFloat(item.Price, 64)
		if err != nil {
			return nil, nil, err
		}
		size, err := strconv.ParseFloat(item.Size, 64)
		if err != nil {
			return nil, nil, err
		}
		prices = append(prices, price)
		quantities = append(quantities, size)
	}
	return prices, quantities, nil
}

func parsePolymarketTimestamp(ts string) int64 {
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return 0
	}
	return t
}
