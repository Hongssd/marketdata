package marketdata

import (
	"github.com/Hongssd/myokxapi"
	"github.com/robfig/cron/v3"
)

type okxCommon struct {
	InstIdToInstTypeMap MySyncMap[string, string]
}

var okx_common = (&okxCommon{}).initCommon()

func (o *okxCommon) initCommon() *okxCommon {
	o.InstIdToInstTypeMap = NewMySyncMap[string, string]()
	o.refreshOkxExchangeInfo()
	c := cron.New(cron.WithSeconds())
	//每隔1小时更新一次交易规范
	_, err := c.AddFunc("0 0 */1 * * *", func() {
		o.refreshOkxExchangeInfo()
	})
	if err != nil {
		log.Error(err)
		return o
	}
	c.Start()

	return o
}

func (o *okxCommon) refreshOkxExchangeInfo() {
	spotExchangeInfo, err := (&myokxapi.MyOkx{}).NewRestClient("", "", "").
		PublicRestClient().NewPublicRestPublicInstruments().InstType(OKX_SPOT.String()).Do()
	if err != nil {
		log.Error(err)
		o.refreshOkxExchangeInfo()
		return
	}
	for _, symbolInfo := range spotExchangeInfo.Data {
		o.InstIdToInstTypeMap.Store(symbolInfo.InstId, OKX_SPOT.String())
	}

	futureExchangeInfo, err := (&myokxapi.MyOkx{}).NewRestClient("", "", "").
		PublicRestClient().NewPublicRestPublicInstruments().InstType(OKX_FUTURE.String()).Do()
	if err != nil {
		log.Error(err)
		o.refreshOkxExchangeInfo()
		return
	}
	for _, symbolInfo := range futureExchangeInfo.Data {
		o.InstIdToInstTypeMap.Store(symbolInfo.InstId, OKX_FUTURE.String())
	}

	swapExchangeInfo, err := (&myokxapi.MyOkx{}).NewRestClient("", "", "").
		PublicRestClient().NewPublicRestPublicInstruments().InstType(OKX_SWAP.String()).Do()
	if err != nil {
		log.Error(err)
		o.refreshOkxExchangeInfo()
		return
	}
	for _, symbolInfo := range swapExchangeInfo.Data {
		o.InstIdToInstTypeMap.Store(symbolInfo.InstId, OKX_SWAP.String())
	}

	optionUlys := []string{"BTC-USD", "ETH-USD"}
	for _, uly := range optionUlys {
		optionExchangeInfo, err := (&myokxapi.MyOkx{}).NewRestClient("", "", "").
			PublicRestClient().NewPublicRestPublicInstruments().InstType(OKX_OPTION.String()).Uly(uly).Do()
		if err != nil {
			log.Error(err)
			o.refreshOkxExchangeInfo()
			return
		}
		for _, symbolInfo := range optionExchangeInfo.Data {
			o.InstIdToInstTypeMap.Store(symbolInfo.InstId, OKX_OPTION.String())
		}
	}
}

func (o *okxCommon) GetAccountTypeFromSymbol(symbol string) string {
	if instType, ok := o.InstIdToInstTypeMap.Load(symbol); ok {
		return instType
	}
	return ""
}
