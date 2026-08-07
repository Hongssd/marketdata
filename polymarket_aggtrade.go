package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mypolymarketapi"
)

type PolymarketAggTrade struct {
	parent               *PolymarketMarketData
	perConnSubNum        int64
	perSubMaxLen         int
	subChannelBufferSize int
	Exchange             Exchange

	subscriberSeq atomic.Int64

	AggTradeMap     *MySyncMap[string, *AggTrade]
	WsClientListMap *MySyncMap[*mypolymarketapi.MarketWsStreamClient, *int64]
	WsClientMap     *MySyncMap[string, *mypolymarketapi.MarketWsStreamClient]
	SubscriberIDMap *MySyncMap[string, string]
	CallBackMap     *MySyncMap[string, func(aggTrade *AggTrade, err error)]
}

func (pm *PolymarketMarketData) newPolymarketAggTrade(config PolymarketAggTradeConfig) *PolymarketAggTrade {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 50
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 100
	}
	if config.SubChannelBufferSize == 0 {
		config.SubChannelBufferSize = 512
	}

	return &PolymarketAggTrade{
		parent:               pm,
		perConnSubNum:        config.PerConnSubNum,
		perSubMaxLen:         config.PerSubMaxLen,
		subChannelBufferSize: config.SubChannelBufferSize,
		Exchange:             POLYMARKET,
		AggTradeMap:          GetPointer(NewMySyncMap[string, *AggTrade]()),
		WsClientListMap:      GetPointer(NewMySyncMap[*mypolymarketapi.MarketWsStreamClient, *int64]()),
		WsClientMap:          GetPointer(NewMySyncMap[string, *mypolymarketapi.MarketWsStreamClient]()),
		SubscriberIDMap:      GetPointer(NewMySyncMap[string, string]()),
		CallBackMap:          GetPointer(NewMySyncMap[string, func(aggTrade *AggTrade, err error)]()),
	}
}

func (p *PolymarketAggTrade) GetLastAggTrade(assetID string) (*AggTrade, error) {
	aggTrade, ok := p.AggTradeMap.Load(assetID)
	if !ok {
		return nil, fmt.Errorf("asset_id:%s aggTrade not found", assetID)
	}
	return aggTrade, nil
}

func (p *PolymarketAggTrade) GetCurrentOrNewWsClient() (*mypolymarketapi.MarketWsStreamClient, error) {
	if p.parent == nil {
		return nil, errors.New("parent market data is nil")
	}
	// last_trade_price 对 level 无特殊要求，沿用默认 level=2。
	return p.parent.GetCurrentOrNewWsClient(p.perConnSubNum, p.WsClientListMap, 2)
}

func (p *PolymarketAggTrade) SubscribeAggTrade(assetID string) error {
	return p.SubscribeAggTradeWithCallBack(assetID, nil)
}

func (p *PolymarketAggTrade) SubscribeAggTrades(assetIDs []string) error {
	return p.SubscribeAggTradesWithCallBack(assetIDs, nil)
}

func (p *PolymarketAggTrade) SubscribeAggTradeWithCallBack(assetID string, callback func(aggTrade *AggTrade, err error)) error {
	return p.SubscribeAggTradesWithCallBack([]string{assetID}, callback)
}

func (p *PolymarketAggTrade) SubscribeAggTradesWithCallBack(assetIDs []string, callback func(aggTrade *AggTrade, err error)) error {
	if len(assetIDs) == 0 {
		return errors.New("assetIDs is empty")
	}
	log.Infof("开始订阅Polymarket归集交易流，资产ID数:%d, 总订阅数:%d", len(assetIDs), len(assetIDs))

	batchSize := p.perSubMaxLen
	if batchSize <= 0 {
		batchSize = len(assetIDs)
	}

	if len(assetIDs) > batchSize {
		for i := 0; i < len(assetIDs); i += batchSize {
			end := i + batchSize
			if end > len(assetIDs) {
				end = len(assetIDs)
			}
			tempAssetIDs := assetIDs[i:end]
			client, err := p.GetCurrentOrNewWsClient()
			if err != nil {
				return err
			}
			err = p.subscribePolymarketAggTradeMultiple(client, tempAssetIDs, callback)
			if err != nil {
				return err
			}
			currentCount, ok := p.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Polymarket归集交易流分批订阅成功，此次assetIDs:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", tempAssetIDs, len(tempAssetIDs), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := p.GetCurrentOrNewWsClient()
		if err != nil {
			return err
		}
		err = p.subscribePolymarketAggTradeMultiple(client, assetIDs, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("Polymarket归集交易流订阅结束，资产ID数:%d, 总订阅数:%d", len(assetIDs), len(assetIDs))
	return nil
}

func (p *PolymarketAggTrade) subscribePolymarketAggTradeMultiple(
	wsClient *mypolymarketapi.MarketWsStreamClient,
	assetIDs []string,
	callback func(aggTrade *AggTrade, err error),
) error {
	subscriberID := p.nextSubscriberID()
	tradeChan, err := wsClient.SubscribeLastTradePrice(subscriberID, assetIDs, p.subChannelBufferSize)
	if err != nil {
		return err
	}

	for _, assetID := range assetIDs {
		p.WsClientMap.Store(assetID, wsClient)
		p.SubscriberIDMap.Store(assetID, subscriberID)
		p.CallBackMap.Store(assetID, callback)
	}

	go func() {
		for trade := range tradeChan {
			aggTrade, convErr := p.convertToAggTrade(trade)
			if convErr != nil {
				log.Error(convErr)
				if callback != nil {
					callback(nil, convErr)
				}
				continue
			}
			p.AggTradeMap.Store(trade.AssetID, aggTrade)
			if callback != nil {
				callback(aggTrade, nil)
			}
		}
	}()

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

func (p *PolymarketAggTrade) convertToAggTrade(trade mypolymarketapi.WsMarketLastTradePrice) (*AggTrade, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("asset_id:%s parse price failed: %w", trade.AssetID, err)
	}
	quantity, err := strconv.ParseFloat(trade.Size, 64)
	if err != nil {
		return nil, fmt.Errorf("asset_id:%s parse size failed: %w", trade.AssetID, err)
	}

	now := time.Now().UnixMilli()
	targetTs := parsePolymarketTimestamp(trade.Timestamp) + p.parent.GetServerTimeDelta()
	if targetTs <= 0 {
		targetTs = now
	}
	if targetTs > now {
		targetTs = now
	}

	// Polymarket side 表示 taker 方向；若 taker 为 SELL，则买方为 maker。
	isMarket := strings.EqualFold(trade.Side, "SELL")
	aid := trade.TransactionHash
	if aid == "" {
		aid = fmt.Sprintf("%s-%d", trade.AssetID, targetTs)
	}

	return &AggTrade{
		AId:         aid,
		Exchange:    p.Exchange.String(),
		AccountType: "",
		Symbol:      trade.AssetID,
		Timestamp:   targetTs,
		Price:       price,
		Quantity:    quantity,
		First:       0,
		Last:        0,
		TradeTime:   targetTs,
		IsMarket:    isMarket,
	}, nil
}

func (p *PolymarketAggTrade) nextSubscriberID() string {
	return fmt.Sprintf("pm-at-%d", p.subscriberSeq.Add(1))
}

func (p *PolymarketAggTrade) Close() {
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

	p.AggTradeMap.Clear()
	p.WsClientListMap.Clear()
	p.WsClientMap.Clear()
	p.SubscriberIDMap.Clear()
	p.CallBackMap.Clear()
}
