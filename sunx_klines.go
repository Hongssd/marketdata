package marketdata

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hongssd/mysunxapi"
)

type SunxKline struct {
	parent    *SunxMarketData
	SwapKline *sunxKlineBase
}

type sunxKlineBase struct {
	parent *SunxKline
	SunxWsClientBase
	Exchange    Exchange
	AccountType SunxAccountType
	KlineMap    *MySyncMap[string, *Kline]
	WsClientMap *MySyncMap[string, *mysunxapi.PublicWsStreamClient]
	SubMap      *MySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketCommonReq, mysunxapi.WsKline]]
	CallBackMap *MySyncMap[string, func(kline *Kline, err error)]
}

func (s *SunxKline) getBaseMapFromAccountType(accountType SunxAccountType) (*sunxKlineBase, error) {
	switch accountType {
	case SUNX_SWAP:
		return s.SwapKline, nil
	}
	return nil, ErrorAccountType
}

func (s *SunxKline) newSunxKlineBase(config SunxKlineConfigBase) *sunxKlineBase {
	if config.PerSubMaxLen == 0 {
		config.PerSubMaxLen = 100
	}
	if config.PerConnSubNum == 0 {
		config.PerConnSubNum = 1000
	}
	return &sunxKlineBase{
		Exchange: SUNX,
		SunxWsClientBase: SunxWsClientBase{
			perConnSubNum:   config.PerConnSubNum,
			perSubMaxLen:    config.PerSubMaxLen,
			WsClientListMap: GetPointer(NewMySyncMap[*mysunxapi.PublicWsStreamClient, *int64]()),
		},
		KlineMap:    GetPointer(NewMySyncMap[string, *Kline]()),
		WsClientMap: GetPointer(NewMySyncMap[string, *mysunxapi.PublicWsStreamClient]()),
		SubMap:      GetPointer(NewMySyncMap[string, *mysunxapi.Subscription[mysunxapi.WsMarketCommonReq, mysunxapi.WsKline]]()),
		CallBackMap: GetPointer(NewMySyncMap[string, func(kline *Kline, err error)]()),
	}
}

// 封装好的获取K线方法
func (s *SunxKline) GetLastKline(accountType SunxAccountType, symbol, interval string) (*Kline, error) {
	symbolKey := symbol + "_" + interval

	bmap, err := s.getBaseMapFromAccountType(accountType)
	if err != nil {
		return nil, err
	}

	kline, ok := bmap.KlineMap.Load(symbolKey)
	if !ok {
		err := fmt.Errorf("%s symbol:%s kline not found", accountType, symbol)
		return nil, err
	}
	return kline, nil
}

// 获取当前或新建ws客户端
func (s *SunxKline) GetCurrentOrNewWsClient(accountType SunxAccountType) (*mysunxapi.PublicWsStreamClient, error) {
	switch accountType {
	case SUNX_SWAP:
		return s.SwapKline.GetCurrentOrNewWsClient(accountType)
	default:
		return nil, ErrorAccountType
	}
}

func (s *sunxKlineBase) subscribeSunxKline(sunxWsClient *mysunxapi.PublicWsStreamClient, symbol, interval string, callback func(kline *Kline, err error)) error {
	return s.subscribeSunxKlineMultiple(sunxWsClient, []string{symbol}, []string{interval}, callback)
}

// 订阅SunXK线底层执行
func (s *sunxKlineBase) subscribeSunxKlineMultiple(sunxWsClient *mysunxapi.PublicWsStreamClient, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	sunxSub, err := sunxWsClient.SubscribeMarketKline(symbols, intervals, true)
	if err != nil {
		log.Error(err)
		return err
	}

	for _, symbol := range symbols {
		for _, interval := range intervals {
			symbolKey := symbol + "_" + interval
			s.WsClientMap.Store(symbolKey, sunxWsClient)
			s.SubMap.Store(symbolKey, sunxSub)
			s.CallBackMap.Store(symbolKey, callback)
		}
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
				split := strings.Split(r.Ch, ".")
				if len(split) < 2 {
					log.Error("subscribe err, r.ch could be empty")
					continue
				}
				interval := split[3]
				symbol := split[1]
				symbolKey := symbol + "_" + interval
				timestampUnix := result.Id
				timestamp := timestampUnix * 1000
				now := time.Now().UnixMilli()
				targetTs := timestamp + s.parent.parent.GetServerTimeDelta()
				if targetTs > now {
					targetTs = now
				}
				//保存至Kline
				kline := &Kline{
					Timestamp:            targetTs,
					Exchange:             s.Exchange.String(),
					AccountType:          s.AccountType.String(),
					Symbol:               symbol,
					Interval:             interval,
					StartTime:            timestamp,
					Open:                 result.Open,
					High:                 result.High,
					Low:                  result.Low,
					Close:                result.Close,
					Volume:               result.Vol,
					CloseTime:            timestamp + SunxInterval(interval).Millisecond() - 1,
					TransactionVolume:    result.TradeTurnover,
					TransactionNumber:    result.Count,
					BuyTransactionVolume: 0,
					BuyTransactionAmount: 0,
					Confirm:              false,
				}
				s.KlineMap.Store(symbolKey, kline)
				if callback != nil {
					callback(kline, nil)
				}
			case <-sunxSub.CloseChan():
				log.Info("订阅已关闭: ", sunxSub.SubReqs)
				return
			}
		}
	}()

	currentCount := int64(len(symbols) * len(intervals))
	count, ok := s.WsClientListMap.Load(sunxWsClient)
	if !ok {
		initCount := int64(0)
		count = &initCount
		s.WsClientListMap.Store(sunxWsClient, &initCount)
	}
	atomic.AddInt64(count, currentCount)
	return nil
}

// 取消订阅Sunx K线
func (s *sunxKlineBase) UnSubscribeSunxKline(symbol, interval string) error {
	symbolKey := symbol + "_" + interval
	sunxSub, ok := s.SubMap.Load(symbolKey)
	if !ok {
		return nil
	}
	return sunxSub.Unsubscribe()
}

// 订阅K线
func (s *SunxKline) SubscribeKline(accountType SunxAccountType, symbol, interval string) error {
	return s.SubscribeKlineWithCallBack(accountType, symbol, interval, nil)
}

// 批量订阅K线
func (s *SunxKline) SubscribeKlines(accountType SunxAccountType, symbols, intervals []string) error {
	return s.SubscribeKlinesWithCallBack(accountType, symbols, intervals, nil)
}

// 订阅K线并带上回调
func (s *SunxKline) SubscribeKlineWithCallBack(accountType SunxAccountType, symbol, interval string, callback func(kline *Kline, err error)) error {
	return s.SubscribeKlinesWithCallBack(accountType, []string{symbol}, []string{interval}, callback)
}

// 批量订阅K线并带上回调
func (s *SunxKline) SubscribeKlinesWithCallBack(accountType SunxAccountType, symbols, intervals []string, callback func(kline *Kline, err error)) error {
	log.Infof("开始订阅K线%s，交易对数:%d, 周期数:%d, 总订阅数:%d", accountType, len(symbols), len(intervals), len(symbols)*len(intervals))

	var currentSunxKlineBase *sunxKlineBase
	switch accountType {
	case SUNX_SWAP:
		currentSunxKlineBase = s.SwapKline
	default:
		return ErrorAccountType
	}

	//订阅总数超过LEN次，分批订阅
	LEN := currentSunxKlineBase.perSubMaxLen
	if len(symbols)*len(intervals) > LEN {
		for i := 0; i < len(symbols); i += LEN / len(intervals) {
			end := i + LEN/len(intervals)
			if end > len(symbols) {
				end = len(symbols)
			}
			tempSymbols := symbols[i:end]
			client, err := s.GetCurrentOrNewWsClient(accountType)
			if err != nil {
				return err
			}
			err = currentSunxKlineBase.subscribeSunxKlineMultiple(client, tempSymbols, intervals, callback)
			if err != nil {
				return err
			}
			currentCount, ok := currentSunxKlineBase.WsClientListMap.Load(client)
			if !ok {
				return errors.New("WsClientListMap Load error")
			}
			log.Infof("K线%s分批订阅成功，此次订阅交易对:%v, 此次订阅周期:%s, 总数:%d，当前链接总订阅数:%d, 等待1秒后继续订阅...", accountType, tempSymbols, intervals, len(tempSymbols)*len(intervals), *currentCount)
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		client, err := s.GetCurrentOrNewWsClient(accountType)
		if err != nil {
			return err
		}
		err = currentSunxKlineBase.subscribeSunxKlineMultiple(client, symbols, intervals, callback)
		if err != nil {
			return err
		}
	}
	log.Infof("K线订阅结束，交易对数:%d, 周期数:%d, 总订阅数:%d", len(symbols), len(intervals), len(symbols)*len(intervals))
	return nil
}

func (s *sunxKlineBase) Close() {
	s.SunxWsClientBase.close()

	s.KlineMap.Clear()
	s.WsClientMap.Clear()
	s.SubMap.Clear()
	s.CallBackMap.Clear()
}

func (s *SunxKline) Close() {
	s.SwapKline.Close()
}
