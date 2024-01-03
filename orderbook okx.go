package marketdata

// import (
// 	"fmt"
// 	"math"
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"time"
// 	"tradingx-strategy-service/constant"
// 	"tradingx-strategy-service/model"
// 	"tradingx-strategy-service/pkg/mylog"
// 	"tradingx-strategy-service/pkg/myutils"

// 	"github.com/Hongssd/mybinanceapi"
// 	"github.com/Hongssd/myokxapi"
// 	"github.com/robfig/cron/v3"
// 	"github.com/shopspring/decimal"
// 	"github.com/sirupsen/logrus"
// )

// const BINANCE_REST_LIMIT_PER_MINUTE = int64(300)

// var currentBinanceRestCount = int64(0)

// // Cache
// var BinanceDepthOrderBookCacheMap = myutils.NewMySyncMap[string, map[int64]*mybinanceapi.WsDepth]()

// // var OkxDepthOrderBookCacheMap = myutils.NewMySyncMap[string, map[int64]*myokxapi.WsBooks]()

// // OrderBook RBTreeMap
// var BinanceDepthOrderBookRBTreeMap = myutils.NewMySyncMap[string, *OrderBook]()
// var OkxDepthOrderBookRBTreeMap = myutils.NewMySyncMap[string, *OrderBook]()

// // DepthReady
// var BinanceDepthOrderBookReadyUpdateIdMap = myutils.NewMySyncMap[string, int64]()
// var OkxDepthOrderBookReadyUpdateIdMap = myutils.NewMySyncMap[string, int64]()

// // LastDepth
// var BinanceDepthOrderBookMap = myutils.NewMySyncMap[string, *model.Depth]()
// var OkxDepthOrderBookMap = myutils.NewMySyncMap[string, *model.Depth]()

// // LastUpdateId
// var BinanceDepthOrderBookLastUpdateIdMap = myutils.NewMySyncMap[string, int64]()
// var OkxDepthOrderBookLastUpdateIdMap = myutils.NewMySyncMap[string, int64]()

// // WsClientMap SubMap
// var BinanceDepthWsClientMap = myutils.NewMySyncMap[string, *mybinanceapi.FutureWsStreamClient]()
// var OkxDepthWsClientMap = myutils.NewMySyncMap[string, *myokxapi.PublicWsStreamClient]()
// var BinanceDepthSubMap = myutils.NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()
// var OkxDepthSubMap = myutils.NewMySyncMap[string, *myokxapi.Subscription[myokxapi.WsBooks]]()

// var BinanceDepthIsInitActionMu = myutils.NewMySyncMap[string, *sync.Mutex]()

// // 封装好的获取深度方法
// func GetBinanceDepth(symbol string, level int, timeoutMilli int64) (*model.Depth, error) {
// 	depth, ok := BinanceDepthOrderBookMap.Load(symbol)
// 	if !ok {
// 		err := fmt.Errorf("symbol:%s depth not found", symbol)
// 		// log.Error(err)
// 		return nil, err
// 	}
// 	orderBook, ok := BinanceDepthOrderBookRBTreeMap.Load(symbol)
// 	if !ok {
// 		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
// 		log.Error(err)
// 		return nil, err
// 	}

// 	newDepth, err := orderBook.LoadToDepth(depth, level)
// 	if err != nil {
// 		return nil, err
// 	}

// 	//如果超时限制大于0 判断深度是否超时
// 	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
// 		err := fmt.Errorf("symbol:%s depth timeout", symbol)
// 		return nil, err
// 	}
// 	return newDepth, nil
// }

// func GetOkxDepth(symbol string, level int, timeoutMilli int64) (*model.Depth, error) {
// 	depth, ok := OkxDepthOrderBookMap.Load(symbol)
// 	if !ok {
// 		err := fmt.Errorf("symbol:%s depth not found", symbol)
// 		// log.Error(err)
// 		return nil, err
// 	}
// 	orderBook, ok := OkxDepthOrderBookRBTreeMap.Load(symbol)
// 	if !ok {
// 		err := fmt.Errorf("symbol:%s bidMap not found", symbol)
// 		log.Error(err)
// 		return nil, err
// 	}
// 	newDepth, err := orderBook.LoadToDepth(depth, level)
// 	if err != nil {
// 		return nil, err
// 	}
// 	//如果超时限制大于0 判断深度是否超时
// 	if timeoutMilli > 0 && time.Now().UnixMilli()-newDepth.Timestamp > timeoutMilli {
// 		err := fmt.Errorf("symbol:%s depth timeout", symbol)
// 		return nil, err
// 	}
// 	return newDepth, nil
// }

// func StartGetDepthOrderBookData() {
// 	LoadSymbol()
// 	newlog := mylog.NewLogger("orderbook")
// 	newlog.SetLevel(logrus.InfoLevel)
// 	mybinanceapi.SetLogger(newlog)
// 	myokxapi.SetLogger(newlog)
// 	binanceSymbols := []string{}
// 	okxSymbols := []string{}

// 	c := cron.New(cron.WithSeconds())
// 	refresh := func() {
// 		// log.Infof("[%d,%d]更新服务器时间开始...", BinanceServerTimeDelta, OkxServerTimeDelta)
// 		err := RefreshBinanceDelta()
// 		if err != nil {
// 			log.Error(err)
// 		}
// 		err = RefreshOkxDelta()
// 		if err != nil {
// 			log.Error(err)
// 		}
// 		// log.Infof("[%dms,%dms]更新服务器时间结束...", BinanceServerTimeDelta, OkxServerTimeDelta)
// 	}
// 	log.Info("启动服务器时间定时任务....")
// 	refresh()

// 	//每隔1秒更新一次服务器时间
// 	_, err := c.AddFunc("*/1 * * * * *", refresh)
// 	if err != nil {
// 		log.Error(err)
// 		return
// 	}

// 	_, err = c.AddFunc("*/10 * * * * *", func() {
// 		log.Infof("当前服务器延迟[BINANCE: %dms, OKX: %dms]", BinanceServerTimeDelta, OkxServerTimeDelta)
// 	})
// 	if err != nil {
// 		log.Error(err)
// 		return
// 	}

// 	//每隔1分钟刷新一次请求次数
// 	_, err = c.AddFunc("0 */1 * * * *", func() {
// 		// log.Info("刷新币安深度请求计数器...")
// 		atomic.StoreInt64(&currentBinanceRestCount, 0)
// 	})
// 	if err != nil {
// 		log.Error(err)
// 		return
// 	}
// 	c.Start()

// 	for _, s := range ArbitrageSymbolList {
// 		if bs, ok := SymbolToBinanceMap.Load(s); ok {
// 			binanceSymbols = append(binanceSymbols, bs)
// 		}
// 		if os, ok := SymbolToOkxMap.Load(s); ok {
// 			okxSymbols = append(okxSymbols, os)
// 		}
// 	}

// 	log.Info("币安订阅交易对数量:", len(binanceSymbols))
// 	log.Info("OKX订阅交易对数量:", len(okxSymbols))

// 	if len(binanceSymbols) == 0 || len(okxSymbols) == 0 {
// 		return
// 	}
// 	// binanceSymbols = []string{"BTCUSDT"}
// 	// okxSymbols = []string{"BTC-USDT-SWAP"}

// 	perConnSubNum := 7
// 	//按交易对总量分割，每7个启动一个ws连接并进行订阅
// 	for i := 0; i < len(binanceSymbols); i += perConnSubNum {
// 		bsymbols := binanceSymbols[i:int(math.Min(float64(i+perConnSubNum), float64(len(binanceSymbols))))]
// 		osymbols := okxSymbols[i:int(math.Min(float64(i+perConnSubNum), float64(len(okxSymbols))))]

// 		binanceWsClient := binance.NewFutureWsStreamClient()
// 		okxWsClient := okx.NewPublicWsStreamClient()
// 		binanceWsClient.OpenConn()
// 		okxWsClient.OpenConn()

// 		//Binance 逐个交易对进行订阅
// 		log.Info("开启币安订阅: ", bsymbols)
// 		for _, bsymbol := range bsymbols {
// 			SubscribeBinanceDepth(binanceWsClient, bsymbol)
// 		}

// 		//Okx 逐个交易对进行订阅 订阅前先执行登陆
// 		// APIKey := "3f70cb33-4a83-47ff-a89f-efa040739177"
// 		// secretkey := "B0AD485AE5AFEFF09E307ED5451FEE11"
// 		// Passphrase := "Lsokexhfmm_1"
// 		APIKey := "ec45257a-1fab-4ab5-9b05-f10a4402fed6"
// 		secretkey := "885BEF50083747B56E38C2E0F5D4564D"
// 		Passphrase := "Lsokexarb@2023"
// 		err = okxWsClient.Login(okx.NewRestClient(APIKey, secretkey, Passphrase))
// 		if err != nil {
// 			log.Error(err)
// 			return
// 		}

// 		log.Info("开启OKX订阅: ", osymbols)
// 		for _, osymbol := range osymbols {
// 			SubscribeOkxDepth(okxWsClient, osymbol)
// 		}
// 	}
// }

// func DepthContractSizeToQuantity(depth *model.Depth, contractSize float64) *model.Depth {
// 	for _, bid := range depth.Bids {
// 		bid.Quantity = decimal.NewFromFloat(bid.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
// 	}
// 	for _, ask := range depth.Asks {
// 		ask.Quantity = decimal.NewFromFloat(ask.Quantity).Mul(decimal.NewFromFloat(contractSize)).InexactFloat64()
// 	}
// 	return depth
// }

// // 订阅币安深度
// func SubscribeBinanceDepth(binanceWsClient *mybinanceapi.FutureWsStreamClient, bsymbol string) {
// 	Symbol, _ := BinanceToSymbolMap.Load(bsymbol)
// 	BinanceDepthWsClientMap.Store(Symbol, binanceWsClient)
// 	binanceSub, err := binanceWsClient.SubscribeIncrementDepth(bsymbol, "0ms")
// 	for err != nil {
// 		log.Error(err)
// 		if strings.Contains(err.Error(), "close") {
// 			binanceWsClient, ok := BinanceDepthWsClientMap.Load(Symbol)
// 			if ok {
// 				binanceWsClient.OpenConn()
// 			}
// 		}
// 		time.Sleep(1000 * time.Millisecond)
// 		binanceSub, err = binanceWsClient.SubscribeIncrementDepth(bsymbol, "0ms")
// 	}
// 	BinanceDepthWsClientMap.Store(Symbol, binanceWsClient)
// 	BinanceDepthSubMap.Store(Symbol, binanceSub)

// 	//初始化深度锁
// 	BinanceDepthIsInitActionMu.Store(bsymbol, &sync.Mutex{})

// 	go func() {
// 		for {
// 			// log.Info("next binanceSub...")
// 			select {
// 			case err := <-binanceSub.ErrChan():
// 				log.Error(err)
// 			case result := <-binanceSub.ResultChan():
// 				Symbol, _ := BinanceToSymbolMap.Load(result.Symbol)
// 				//检测深度是否初始化
// 				_, err := checkBinanceDepthIsReady(Symbol)
// 				if err != nil {
// 					//直接存入深度缓存
// 					saveBinanceDepthCache(result)
// 					continue
// 				}
// 				//保存至OrderBook
// 				err = saveBinanceDepthOrderBook(result, false)
// 				if err != nil {
// 					//保存失败，清空相关数据并重新初始化深度，不需要重新订阅
// 					//清空相关数据
// 					BinanceDepthOrderBookReadyUpdateIdMap.Delete(Symbol)
// 					BinanceDepthOrderBookMap.Delete(Symbol)
// 					BinanceDepthOrderBookLastUpdateIdMap.Delete(Symbol)
// 					BinanceDepthOrderBookCacheMap.Delete(Symbol)
// 					BinanceDepthOrderBookRBTreeMap.Delete(Symbol)

// 					//重新初始化深度
// 					go initBinanceDepthFunc(result.Symbol)
// 					continue
// 				}
// 			case <-binanceSub.CloseChan():
// 				log.Info("订阅已关闭: ", binanceSub.Params)
// 				return
// 			}
// 		}
// 	}()

// 	//初始化深度池
// 	err = initBinanceDepthFunc(bsymbol)
// 	for err != nil {
// 		log.Error(err)
// 		time.Sleep(5 * time.Second)
// 		err = initBinanceDepthFunc(bsymbol)
// 	}
// 	log.Info("初始化币安深度成功: ", bsymbol)
// }

// // 订阅OKX深度
// func SubscribeOkxDepth(okxWsClient *myokxapi.PublicWsStreamClient, osymbol string) {
// 	Symbol, _ := OkxToSymbolMap.Load(osymbol)

// 	okxSub, err := okxWsClient.SubscribeBooks(osymbol, myokxapi.WS_BOOKS_UPDATE_50_10MS)
// 	for err != nil {
// 		log.Error(err)
// 		time.Sleep(1000 * time.Millisecond)
// 		okxSub, err = okxWsClient.SubscribeBooks(osymbol, myokxapi.WS_BOOKS_UPDATE_50_10MS)
// 	}
// 	OkxDepthWsClientMap.Store(Symbol, okxWsClient)
// 	OkxDepthSubMap.Store(Symbol, okxSub)

// 	go func() {
// 		for {
// 			select {
// 			case err := <-okxSub.ErrChan():
// 				log.Error(err)
// 			case result := <-okxSub.ResultChan():
// 				Symbol, _ := OkxToSymbolMap.Load(result.InstId)
// 				switch result.Action {
// 				case "snapshot":
// 					//首次接收到推送全量数据，清除其他数据，并直接初始化
// 					OkxDepthOrderBookReadyUpdateIdMap.Delete(Symbol)
// 					OkxDepthOrderBookMap.Delete(Symbol)
// 					OkxDepthOrderBookLastUpdateIdMap.Delete(Symbol)
// 					OkxDepthOrderBookRBTreeMap.Delete(Symbol)

// 					initOkxDepthFunc(result)
// 				case "update":
// 					//增量数据更新，需要进行校验
// 					_, err := checkOkxDepthIsReady(Symbol)
// 					if err != nil {
// 						//首次全量数据丢失，直接重新订阅
// 						go func() {
// 							err := ReSubscribeOkxDepth(osymbol)
// 							for err != nil {
// 								log.Error(err)
// 								time.Sleep(1000 * time.Millisecond)
// 								err = ReSubscribeOkxDepth(osymbol)
// 							}
// 						}()
// 						continue
// 					}

// 					err = saveOkxDepthOrderBook(result)
// 					if err != nil {
// 						//保存增量数据失败，直接重新订阅
// 						go func() {
// 							err := ReSubscribeOkxDepth(osymbol)
// 							for err != nil {
// 								log.Error(err)
// 								time.Sleep(1000 * time.Millisecond)
// 								err = ReSubscribeOkxDepth(osymbol)
// 							}
// 						}()
// 						continue
// 					}
// 				}

// 			case <-okxSub.CloseChan():
// 				log.Info("订阅已关闭: ", okxSub.Args)
// 				return
// 			}
// 		}
// 	}()
// }

// // 取消订阅币安深度
// func UnSubscribeBinanceDepth(bsymbol string) error {
// 	Symbol, _ := BinanceToSymbolMap.Load(bsymbol)
// 	binanceSub, ok := BinanceDepthSubMap.Load(Symbol)
// 	if !ok {
// 		return nil
// 	}
// 	return binanceSub.Unsubscribe()
// }

// // 取消订阅OKX深度
// func UnSubscribeOkxDepth(osymbol string) error {
// 	Symbol, _ := OkxToSymbolMap.Load(osymbol)
// 	okxSub, ok := OkxDepthSubMap.Load(Symbol)
// 	if !ok {
// 		return nil
// 	}
// 	return okxSub.Unsubscribe()
// }

// // 重新订阅币安深度
// func ReSubscribeBinanceDepth(bsymbol string) error {
// 	err := UnSubscribeBinanceDepth(bsymbol)
// 	if err != nil {
// 		return err
// 	}
// 	Symbol, _ := BinanceToSymbolMap.Load(bsymbol)
// 	binanceWsClient, ok := BinanceDepthWsClientMap.Load(Symbol)
// 	if !ok {
// 		err := fmt.Errorf("symbol:%s binanceWsClient not found", Symbol)
// 		return err
// 	}
// 	SubscribeBinanceDepth(binanceWsClient, bsymbol)
// 	return nil
// }

// // 重新订阅OKX深度
// func ReSubscribeOkxDepth(bsymbol string) error {
// 	err := UnSubscribeOkxDepth(bsymbol)
// 	if err != nil {
// 		return err
// 	}
// 	Symbol, _ := OkxToSymbolMap.Load(bsymbol)
// 	okxWsClient, ok := OkxDepthWsClientMap.Load(Symbol)
// 	if !ok {
// 		err := fmt.Errorf("symbol:%s okxWsClient not found", Symbol)
// 		return err
// 	}
// 	SubscribeOkxDepth(okxWsClient, bsymbol)
// 	return nil
// }

// // 初始化币安深度
// func initBinanceDepthFunc(binanceSymbol string) error {
// 	mu, ok := BinanceDepthIsInitActionMu.Load(binanceSymbol)
// 	if !ok {
// 		mu = &sync.Mutex{}
// 		BinanceDepthIsInitActionMu.Store(binanceSymbol, mu)
// 	}
// 	if mu.TryLock() {
// 		defer mu.Unlock()
// 	} else {
// 		return nil
// 	}
// 	err := initBinanceDepthOrderBook(binanceSymbol)
// 	for err != nil {
// 		// log.Error(err)
// 		time.Sleep(time.Second * 5)
// 		log.Info("重新初始化币安深度: ", binanceSymbol)
// 		err = initBinanceDepthOrderBook(binanceSymbol)
// 	}
// 	//初始化完毕，将缓存保存至OrderBook
// 	return saveBinanceDepthOrderBookFromCache(binanceSymbol)
// }

// func initOkxDepthFunc(result myokxapi.WsBooks) {
// 	initOkxDepthOrderBook(result)
// }

// // 检测深度是否准备好
// func checkBinanceDepthIsReady(Symbol string) (int64, error) {
// 	readyId, isReady := BinanceDepthOrderBookReadyUpdateIdMap.Load(Symbol)
// 	if !isReady {
// 		err := fmt.Errorf("%s 深度未准备好", Symbol)
// 		return 0, err
// 	}
// 	return readyId, nil
// }
// func checkOkxDepthIsReady(Symbol string) (int64, error) {
// 	readyId, isReady := OkxDepthOrderBookReadyUpdateIdMap.Load(Symbol)
// 	if !isReady {
// 		err := fmt.Errorf("%s 深度未准备好", Symbol)
// 		return 0, err
// 	}
// 	return readyId, nil
// }

// // 初始化深度
// func initBinanceDepthOrderBook(binanceSymbol string) error {
// 	Symbol, _ := BinanceToSymbolMap.Load(binanceSymbol)

// 	if atomic.LoadInt64(&currentBinanceRestCount) >= BINANCE_REST_LIMIT_PER_MINUTE {
// 		return fmt.Errorf("币安rest请求次数超出限制")
// 	}
// 	atomic.AddInt64(&currentBinanceRestCount, 1)
// 	//重新初始化
// 	depth, err := binance.NewFutureRestClient("", "").NewFutureDepth().Symbol(binanceSymbol).Limit(50).Do()
// 	if err != nil {
// 		log.Error(err)
// 		return err
// 	}

// 	orderBook, ok := BinanceDepthOrderBookRBTreeMap.Load(Symbol)
// 	if !ok {
// 		orderBook = NewOrderBook()
// 		BinanceDepthOrderBookRBTreeMap.Store(Symbol, orderBook)
// 	}

// 	//保存至OrderBook
// 	for _, bid := range depth.Bids {
// 		p, q, err := bid.ParseDecimal()
// 		if err != nil {
// 			log.Error(err)
// 			return err
// 		}
// 		orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
// 	}
// 	for _, ask := range depth.Asks {
// 		p, q, err := ask.ParseDecimal()
// 		if err != nil {
// 			log.Error(err)
// 			return err
// 		}
// 		orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
// 	}

// 	BinanceDepthOrderBookReadyUpdateIdMap.Store(Symbol, depth.LastUpdateId)
// 	BinanceDepthOrderBookLastUpdateIdMap.Store(Symbol, depth.LastUpdateId)

// 	return nil
// }
// func initOkxDepthOrderBook(result myokxapi.WsBooks) {
// 	Symbol, _ := OkxToSymbolMap.Load(result.InstId)

// 	orderBook, ok := OkxDepthOrderBookRBTreeMap.Load(Symbol)
// 	if !ok {
// 		orderBook = NewOrderBook()
// 		OkxDepthOrderBookRBTreeMap.Store(Symbol, orderBook)
// 	}
// 	//保存至OrderBook
// 	for _, bid := range result.Bids {
// 		p, _ := decimal.NewFromString(bid.Price)
// 		q, _ := decimal.NewFromString(bid.Quantity)
// 		orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
// 	}
// 	for _, ask := range result.Asks {
// 		p, _ := decimal.NewFromString(ask.Price)
// 		q, _ := decimal.NewFromString(ask.Quantity)
// 		orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
// 	}

// 	OkxDepthOrderBookReadyUpdateIdMap.Store(Symbol, result.SeqId)
// 	OkxDepthOrderBookLastUpdateIdMap.Store(Symbol, result.SeqId)

// }

// // 将Depth缓存保存至OrderBook
// func saveBinanceDepthOrderBookFromCache(binanceSymbol string) error {
// 	Symbol, _ := BinanceToSymbolMap.Load(binanceSymbol)
// 	readyId, err := checkBinanceDepthIsReady(Symbol)
// 	if err != nil {
// 		log.Error(err)
// 		return err
// 	}

// 	//读取缓存到OrderBook
// 	cacheMap, ok := BinanceDepthOrderBookCacheMap.Load(Symbol)
// 	if !ok {
// 		cacheMap = map[int64]*mybinanceapi.WsDepth{}
// 		BinanceDepthOrderBookCacheMap.Store(Symbol, cacheMap)
// 	}

// 	//按照LowerU排序
// 	var cacheList []mybinanceapi.WsDepth
// 	for _, v := range cacheMap {
// 		cacheList = append(cacheList, *v)
// 	}
// 	sort.Sort(SortBinanceWsDepthSlice(cacheList))

// 	targetCacheList := []mybinanceapi.WsDepth{}
// 	for index, v := range cacheList {
// 		if v.UpperU <= readyId && v.LowerU >= readyId {
// 			// log.Warn("v:", v)
// 			err := saveBinanceDepthOrderBook(v, true)
// 			if err != nil {
// 				log.Error(err)
// 				return err
// 			}
// 			targetCacheList = cacheList[index+1:]
// 		}
// 	}

// 	for _, v := range targetCacheList {
// 		err := saveBinanceDepthOrderBook(v, false)
// 		if err != nil {
// 			// log.Error(err)
// 			return err
// 		}
// 	}

// 	return nil
// }

// // 将Depth缓存
// func saveBinanceDepthCache(result mybinanceapi.WsDepth) {
// 	Symbol, _ := BinanceToSymbolMap.Load(result.Symbol)
// 	cacheMap, ok := BinanceDepthOrderBookCacheMap.Load(Symbol)
// 	if !ok {
// 		cacheMap = map[int64]*mybinanceapi.WsDepth{}
// 		BinanceDepthOrderBookCacheMap.Store(Symbol, cacheMap)
// 	}
// 	cacheMap[result.LowerU] = &result
// }

// // 将Depth保存至OrderBook
// func saveBinanceDepthOrderBook(result mybinanceapi.WsDepth, unCheck bool) error {
// 	Symbol, _ := BinanceToSymbolMap.Load(result.Symbol)
// 	if !unCheck {
// 		lastLowerU, ok := BinanceDepthOrderBookLastUpdateIdMap.Load(Symbol)
// 		if ok {
// 			if result.PreU != lastLowerU {
// 				err := fmt.Errorf("%s lastLowerU:%d,preU:%d", Symbol, lastLowerU, result.PreU)
// 				// log.Error(err)
// 				return err
// 			}
// 		}
// 		// log.Warnf("%s lastLowerU:%d,preU:%d", Symbol, lastLowerU, result.PreU)
// 	}

// 	BinanceDepthOrderBookLastUpdateIdMap.Store(Symbol, result.LowerU)

// 	orderBook, ok := BinanceDepthOrderBookRBTreeMap.Load(Symbol)
// 	if !ok {
// 		orderBook = NewOrderBook()
// 		BinanceDepthOrderBookRBTreeMap.Store(Symbol, orderBook)
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for _, bid := range result.Bids {
// 			if bid.Quantity == 0 {
// 				orderBook.RemoveBid(bid.Price)
// 				continue
// 			}
// 			orderBook.PutBid(bid.Price, bid.Quantity)
// 		}
// 	}()
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for _, ask := range result.Asks {
// 			if ask.Quantity == 0 {
// 				orderBook.RemoveAsk(ask.Price)
// 				continue
// 			}
// 			orderBook.PutAsk(ask.Price, ask.Quantity)
// 		}
// 	}()
// 	wg.Wait()

// 	depth := &model.Depth{
// 		AccountType: constant.BINANCE_FUTURE,
// 		Exchange:    constant.BINANCE,
// 		Symbol:      result.Symbol,
// 		Timestamp:   result.Timestamp + BinanceServerTimeDelta,
// 	}
// 	BinanceDepthOrderBookMap.Store(Symbol, depth)

// 	return nil
// }

// // 将Depth保存至OrderBook
// func saveOkxDepthOrderBook(result myokxapi.WsBooks) error {
// 	Symbol, _ := OkxToSymbolMap.Load(result.InstId)

// 	lastSeqId, ok := OkxDepthOrderBookLastUpdateIdMap.Load(Symbol)
// 	if ok {
// 		if result.PrevSeqId != lastSeqId {
// 			err := fmt.Errorf("%s lastSeqId:%d,PrevSeqId:%d", Symbol, lastSeqId, result.PrevSeqId)
// 			return err
// 		}
// 	}

// 	OkxDepthOrderBookLastUpdateIdMap.Store(Symbol, result.SeqId)

// 	orderBook, ok := OkxDepthOrderBookRBTreeMap.Load(Symbol)
// 	if !ok {
// 		orderBook = NewOrderBook()
// 		OkxDepthOrderBookRBTreeMap.Store(Symbol, orderBook)
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for _, bid := range result.Bids {
// 			p, _ := decimal.NewFromString(bid.Price)
// 			q, _ := decimal.NewFromString(bid.Quantity)
// 			if q.IsZero() {
// 				orderBook.RemoveBid(p.InexactFloat64())
// 				continue
// 			}
// 			orderBook.PutBid(p.InexactFloat64(), q.InexactFloat64())
// 		}
// 	}()
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for _, ask := range result.Asks {
// 			p, _ := decimal.NewFromString(ask.Price)
// 			q, _ := decimal.NewFromString(ask.Quantity)
// 			if q.IsZero() {
// 				orderBook.RemoveAsk(p.InexactFloat64())
// 				continue
// 			}
// 			orderBook.PutAsk(p.InexactFloat64(), q.InexactFloat64())
// 		}
// 	}()
// 	wg.Wait()

// 	ts, _ := strconv.ParseInt(result.Ts, 10, 64)
// 	depth := &model.Depth{
// 		AccountType: constant.OKX_FUTURES,
// 		Exchange:    constant.OKX,
// 		Symbol:      result.InstId,
// 		Timestamp:   ts + OkxServerTimeDelta,
// 	}
// 	OkxDepthOrderBookMap.Store(Symbol, depth)
// 	return nil
// }

// type SortBinanceWsDepthSlice []mybinanceapi.WsDepth

// func (s SortBinanceWsDepthSlice) Len() int {
// 	return len(s)
// }

// func (s SortBinanceWsDepthSlice) Swap(i, j int) {
// 	s[i], s[j] = s[j], s[i]
// }

// func (s SortBinanceWsDepthSlice) Less(i, j int) bool {
// 	return s[i].LowerU < s[j].LowerU
// }
