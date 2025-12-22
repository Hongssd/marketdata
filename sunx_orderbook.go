package marketdata

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mysunxapi"
	"github.com/robfig/cron/v3"
	"golang.org/x/sync/errgroup"
)

type SunxOrderBook struct {
	parent        *SunxMarketData
	SwapOrderBook *sunxOrderBookBase
}

type sunxOrderBookBase struct {
	parent                    *SunxOrderBook
	limitRestCountPerMinute   int64 //每分钟rest最大请求数
	currentRestCount          int64 //当前rest请求数
	level                     int   //深度档位
	callBackDepthLevel        int64 //回调深度档位
	callBackDepthTimeoutMilli int64 //回调深度超时时间
	initOrderBookSize         int   //初始OrderBook档位
	SunxWsClientBase
	Exchange                  Exchange
	AccountType               SunxAccountType
	OrderBookCacheMap         *MySyncMap[string, *MySyncMap[int64, *mysunxapi.WsDepthHighFreq]]
	OrderBookRBTreeMap        *MySyncMap[string, OrderBook]
	OrderBookReadyUpdateIdMap *MySyncMap[string, int64]
	OrderBookMap              *MySyncMap[string, *Depth]
	OrderBookLastUpdateIdMap  *MySyncMap[string, int64]
	WsClientMap               *MySyncMap[string, *mysunxapi.PublicWsStreamClient]
	SubMap                    *MySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketDepthHighFreqReq, mysunxapi.WsDepthHighFreq]]
	IsInitActionMu            *MySyncMap[string, *sync.Mutex]
	CallBackMap               *MySyncMap[string, func(depth *Depth, err error)]
}

// 根据类型获取基础
func (s *SunxOrderBook) getBaseMapFromAccountType(accountType SunxAccountType) (*sunxOrderBookBase, error) {
	switch accountType {
	case SUNX_SWAP:
		return s.SwapOrderBook, nil
	}
	return nil, ErrorAccountType
}

// 新建sunx深度基础
func (s *SunxOrderBook) newSunxOrderBookBase(config SunxOrderBookConfigBase) *sunxOrderBookBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 10
	}
	if config.PerConnSubNum == 0 {
		config.PerSubMaxLen = 50
	}
	return &sunxOrderBookBase{
		Exchange: SUNX,
		limitRestCountPerMinute: config.LimitRestCountPerMinute,
		SunxWsClientBase: SunxWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mysunxapi.PublicWsStreamClient, *int64]()),
		},
		level:                     config.Level,
		callBackDepthLevel:        config.CallBackDepthLevel,
		callBackDepthTimeoutMilli: config.CallBackDepthTimeoutMilli,
		initOrderBookSize:         config.InitOrderBookSize,
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, *MySyncMap[int64, *mysunxapi.WsDepthHighFreq]]()),
		OrderBookRBTreeMap:        GetPointer(NewMySyncMap[string, OrderBook]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookMap:              GetPointer(NewMySyncMap[string, *Depth]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mysunxapi.PublicWsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketDepthHighFreqReq, mysunxapi.WsDepthHighFreq]]()),
		IsInitActionMu:            GetPointer(NewMySyncMap[string, *sync.Mutex]()),
		CallBackMap:               GetPointer(NewMySyncMap[string, func(depth *Depth, err error)]()),
	}
}

// 初始化
func (s *SunxOrderBook) init() {
	c := cron.New(cron.WithSeconds())
	//每隔1分钟刷新一次请求次数
	_, err := c.AddFunc("0 */1 * * * *", func() {
		atomic.StoreInt64(&s.SwapOrderBook.currentRestCount, 0)
	})
	if err != nil {
		log.Error(err)
		return
	}
	c.Start()
}

// 获取当前或新建ws客户端
func (s *SunxOrderBook) GetCurrentOrNewWsClient(accountType SunxAccountType) (*mysunxapi.PublicWsStreamClient, error) {
	switch accountType {
	case SUNX_SWAP:
		return s.SwapOrderBook.GetCurrentOrNewWsClient(accountType)
	}
	return nil, ErrorAccountType
}

// 封装好的获取深度方法
func (s *SunxOrderBook) GetDepth(SunxAccountType SunxAccountType, symbol string, level int, timeoutMilli int64) (*Depth, error) {
	bmap, err := s.getBaseMapFromAccountType(SunxAccountType)
	if err != nil {
		return nil, err
	}

	depth, ok := bmap.OrderBookMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s depth not found", symbol)
		return nil, err
	}
	orderBook, ok := bmap.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
		log.Error(err)
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

// 订阅sunx深度底层执行
func (s *sunxOrderBookBase) subscribeSunxDepthMultiple(sunxWsClient *mysunxapi.PublicWsStreamClient, symbols []string, callback func(depth *Depth, err error)) error {
	sunxSub, err := sunxWsClient.SubscribeMarketDepthHighFreq(symbols, []int{s.level}, true)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, symbol := range symbols {
		symbolKey := symbol
		s.WsClientMap.Store(symbolKey, sunxWsClient)
		s.SubMap.Store(symbolKey, sunxSub)
		s.CallBackMap.Store(symbolKey, callback)
	}

	go func() {
		for {
			select {
			case err := <-sunxSub.ErrChan():
				log.Error(err)
				if callback != nil {
					callback(nil, err)
				}
			case r := <-sunxSub.ResultChan():
				result := r.Tick
				split := strings.Split(result.Ch, ".")
				if len(split) < 2 {
					log.Error("subscribe err, r.ch is empty")
					continue
				}
				Symbol := split[1]

				// 检测深度是否初始化
				_, err := s.checkSunxDepthIsReady(Symbol)
				if err != nil {
					// 直接存入深度缓存
					s.saveSunxDepthCache(r)
					continue
				}

				// 检测丢包 pass

				// 保存至OrderBook
				err = s.saveSunxDepthOrderBook(r)
				if err != nil {
					log.Error(err)
					continue
				}

				if callback == nil || s.callBackDepthLevel == 0 {
					continue
				}
				depth, err := s.parent.GetDepth(s.AccountType, Symbol, int(s.callBackDepthLevel), s.callBackDepthTimeoutMilli)
				if err != nil {
					callback(nil, err)
					continue
				}
				depth.UId, depth.PreUId = s.GetUidAndPreUid(r, Symbol)
				callback(depth, err)
			case <-sunxSub.CloseChan():
				log.Info("订阅已关闭: ", sunxSub.SubReqs)
				return
			}
		}
	}()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		// 创建一个新的Group
		g, ctx := errgroup.WithContext(ctx)
		for _, symbol := range symbols {
			symbol := symbol
			g.Go(func() error {
				// 初始化深度池
				err = s.initSunxDepthFunc(symbol)
				if err != nil {
					log.Error(err)
					return err
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			log.Error(err)
			return
		}
	}()

	currentCount := int64(len(symbols))
	count, ok := s.WsClientListMap.Load(sunxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		s.WsClientListMap.Store(sunxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

// 初始化Sunx深度
func (s *sunxOrderBookBase) initSunxDepthFunc(symbol string) error {
	mu, ok := s.IsInitActionMu.Load(symbol)
	if !ok {
		mu = &sync.Mutex{}
		s.IsInitActionMu.Store(symbol, mu)
	}
	if mu.TryLock() {
		defer mu.Unlock()
	} else {
		return nil
	}
	err := s.initSunxDepthOrderBook(symbol)
	for err != nil {
		log.Error(err)
		time.Sleep(time.Second * 1)
		log.Info("重新初始化Sunx深度: ", symbol)
		err = s.initSunxDepthOrderBook(symbol)
	}
	//初始化完毕，将缓存保存至OrderBook
	return s.saveSunxDepthOrderBookFromCache(symbol)
}

// 检测深度是否准备好
func (s *sunxOrderBookBase) checkSunxDepthIsReady(Symbol string) (int64, error) {
	readyId, isReady := s.OrderBookReadyUpdateIdMap.Load(Symbol)
	if !isReady {
		err := fmt.Errorf("%s 深度未准备好", Symbol)
		return 0, err
	}
	return readyId, nil
}

// 初始化深度
func (s *sunxOrderBookBase) initSunxDepthOrderBook(symbol string) error {
	if atomic.LoadInt64(&s.currentRestCount) >= s.limitRestCountPerMinute {
		return fmt.Errorf("Sunxrest请求次数超出限制: %d >= %d", atomic.LoadInt64(&s.currentRestCount), s.limitRestCountPerMinute)
	}
	atomic.AddInt64(&s.currentRestCount, 1)
	orderBook, ok := s.OrderBookRBTreeMap.Load(symbol)
	if !ok {
		orderBook = NewOrderBook()
		s.OrderBookRBTreeMap.Store(symbol, orderBook)
	}
	switch s.AccountType {
	case SUNX_SWAP:
		//重新初始化
		res, err := sunx.NewPublicRestClient().NewPublicRestMarketDepth().
			ContractCode(symbol).Type("step0").Do()
		if err != nil {
			log.Error(err)
			return err
		}
		depth := res.Data
		// 保存至OrderBook
		bidPrices := make([]float64, 0, len(depth.Bids))
		bidQuantities := make([]float64, 0, len(depth.Bids))
		askPrices := make([]float64, 0, len(depth.Asks))
		askQuantities := make([]float64, 0, len(depth.Asks))
		for _, bid := range depth.Bids {
			bidPrices = append(bidPrices, bid.Price)
			bidQuantities = append(bidQuantities, bid.Volume)
		}
		for _, ask := range depth.Asks {
			askPrices = append(askPrices, ask.Price)
			askQuantities = append(askQuantities, ask.Volume)
		}
		orderBook.PutBidLevels(bidPrices, bidQuantities)
		orderBook.PutAskLevels(askPrices, askQuantities)
		s.OrderBookLastUpdateIdMap.Store(symbol, depth.Id)

		log.Infof("[%s#%s] 初始化深度成功: %s ID[%d]\n深度[%v]", s.Exchange, s.AccountType, symbol, depth.Id, depth)
	}
	return nil
}

// 将Depth缓存保存至OrderBook
func (s *sunxOrderBookBase) saveSunxDepthOrderBookFromCache(symbol string) error {
	baseId, ok := s.OrderBookLastUpdateIdMap.Load(symbol)
	if !ok {
		return fmt.Errorf("%s baseId not found", symbol)
	}

	//读取缓存到OrderBook
	cacheMap, ok := s.OrderBookCacheMap.Load(symbol)
	if !ok {
		newMap := NewMySyncMap[int64, *mysunxapi.WsDepthHighFreq]()
		cacheMap = &newMap
		s.OrderBookCacheMap.Store(symbol, cacheMap)
	}

	//从Map中重新读取缓存
	var cacheList []mysunxapi.WsDepthHighFreq
	cacheMap.Range(func(k int64, v *mysunxapi.WsDepthHighFreq) bool {
		if v.Tick.Id > baseId {
			cacheList = append(cacheList, *v)
		}
		return true
	})

	// 排序缓存
	sort.Slice(cacheList, func(i, j int) bool {
		return cacheList[i].Tick.Id < cacheList[j].Tick.Id
	})

	for _, v := range cacheList {
		err := s.saveSunxDepthOrderBook(v)
		if err != nil {
			log.Error(err)
			return err
		}
		baseId = v.Tick.Id
	}

	if _, ok := s.OrderBookRBTreeMap.Load(symbol); ok {
		s.OrderBookReadyUpdateIdMap.Store(symbol, baseId)
	}

	return nil
}

// 将Depth缓存
func (s *sunxOrderBookBase) saveSunxDepthCache(result mysunxapi.WsDepthHighFreq) {
	split := strings.Split(result.Tick.Ch, ".")
	if len(split) < 2 {
		log.Error("subscribe err, r.ch is empty")
		return
	}
	symbol := split[1]
	cacheMap, ok := s.OrderBookCacheMap.Load(symbol)
	if !ok {
		newMap := NewMySyncMap[int64, *mysunxapi.WsDepthHighFreq]()
		cacheMap = &newMap
		s.OrderBookCacheMap.Store(symbol, cacheMap)
	}
	cacheMap.Store(result.Tick.Id, &result)
}

// 将Depth保存至OrderBook
func (s *sunxOrderBookBase) saveSunxDepthOrderBook(result mysunxapi.WsDepthHighFreq) error {
	split := strings.Split(result.Tick.Ch, ".")
	if len(split) < 2 {
		return fmt.Errorf("subscribe err, r.ch is empty")
	}
	Symbol := split[1]

	s.OrderBookLastUpdateIdMap.Store(Symbol, result.Tick.Id)

	orderBook, ok := s.OrderBookRBTreeMap.Load(Symbol)
	if !ok {
		orderBook = NewOrderBook()
		s.OrderBookRBTreeMap.Store(Symbol, orderBook)
	}

	bidPrices := make([]float64, 0, len(result.Tick.Bids))
	bidQuantities := make([]float64, 0, len(result.Tick.Bids))
	askPrices := make([]float64, 0, len(result.Tick.Asks))
	askQuantities := make([]float64, 0, len(result.Tick.Asks))

	for _, bid := range result.Tick.Bids {
		bidPrices = append(bidPrices, bid.Price)
		bidQuantities = append(bidQuantities, bid.Volume)
	}
	for _, ask := range result.Tick.Asks {
		askPrices = append(askPrices, ask.Price)
		askQuantities = append(askQuantities, ask.Volume)
	}

	orderBook.PutBidLevels(bidPrices, bidQuantities)
	orderBook.PutAskLevels(askPrices, askQuantities)

	UId, PreUId := s.GetUidAndPreUid(result, Symbol)
	now := time.Now().UnixMilli()
	targetTs := result.Tick.Ts + s.parent.parent.GetServerTimeDelta()
	if targetTs > now {
		targetTs = now
	}
	depth := &Depth{
		UId:         UId,
		PreUId:      PreUId,
		AccountType: string(s.AccountType),
		Exchange:    string(s.Exchange),
		Symbol:      Symbol,
		Timestamp:   targetTs,
	}
	s.OrderBookMap.Store(Symbol, depth)

	return nil
}

func (s *sunxOrderBookBase) GetUidAndPreUid(result mysunxapi.WsDepthHighFreq, symbol string) (int64, int64) {
	UId := result.Tick.Id
	// 从 OrderBookMap 中查找该 symbol 上一次缓存的 Depth 数据
	PreUId := int64(0)
	if lastDepth, ok := s.OrderBookMap.Load(symbol); ok && lastDepth != nil {
		PreUId = lastDepth.UId
	}
	return UId, PreUId
}

// 订阅深度
func (s *SunxOrderBook) SubscribeSunxOrderBook(accountType SunxAccountType, symbol string) error {
	return s.SubscribeSunxOrderBooksWithCallBack(accountType, []string{symbol}, nil)
}

// 批量订阅深度
func (s *SunxOrderBook) SubscribeSunxOrderBooks(accountType SunxAccountType, symbols []string) error {
	return s.SubscribeSunxOrderBooksWithCallBack(accountType, symbols, nil)
}

// 订阅深度并带上回调
func (s *SunxOrderBook) SubscribeSunxOrderBookWithCallBack(accountType SunxAccountType, symbol string, callback func(depth *Depth, err error)) error {
	return s.SubscribeSunxOrderBooksWithCallBack(accountType, []string{symbol}, callback)
}

// 批量订阅深度并带上回调
func (s *SunxOrderBook) SubscribeSunxOrderBooksWithCallBack(accountType SunxAccountType, symbols []string, callback func(depth *Depth, err error)) error {
	log.Infof("开始订阅Sunx深度%s，交易对数:%d, 总订阅数:%d", accountType, len(symbols), len(symbols))

	var currentSunxOrderBookBase *sunxOrderBookBase

	switch accountType {
	case SUNX_SWAP:
		currentSunxOrderBookBase = s.SwapOrderBook
	default:
		return ErrorAccountType
	}

	LEN := currentSunxOrderBookBase.perSubMaxLen
	if len(symbols) > LEN {
		for i := 0; i < len(symbols); i += LEN {
			end := i + LEN
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := s.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentSunxOrderBookBase.subscribeSunxDepthMultiple(client, tempSymbols, callback)
			if err != nil {
				return err
			}
			currentCount, ok := currentSunxOrderBookBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("Sunx深度%s分批订阅成功，此次订阅交易对:%v, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, len(tempSymbols), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := s.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentSunxOrderBookBase.subscribeSunxDepthMultiple(client, symbols, callback)
		if err != nil {
			return err
		}
	}

	log.Infof("Sunx深度订阅结束，交易对数:%d,  总订阅数:%d", len(symbols), len(symbols))

	return nil
}

func (s *sunxOrderBookBase) Close() {
	s.SunxWsClientBase.close()

	s.OrderBookCacheMap.Clear()
	s.OrderBookRBTreeMap.Clear()
	s.OrderBookReadyUpdateIdMap.Clear()
	s.OrderBookMap.Clear()
	s.OrderBookLastUpdateIdMap.Clear()
	s.WsClientMap.Clear()
	s.SubMap.Clear()
	s.IsInitActionMu.Clear()
	s.CallBackMap.Clear()
}

func (s *SunxOrderBook) Close() {
	s.SwapOrderBook.Close()
}
