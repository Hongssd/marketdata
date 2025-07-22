package marketdata

type Ticker struct {
	Timestamp       int64   `json:"timestamp"`         //毫秒时间戳
	Exchange        string  `json:"exchange"`          //交易所
	AccountType     string  `json:"account_type"`      //类型
	Symbol          string  `json:"symbol"`            //交易对
	LastPrice       float64 `json:"last_price"`        //最新成交价
	Price24hPercent float64 `json:"price_24h_percent"` //24小时价格变化百分比
	PrevPrice24h    float64 `json:"prev_price_24h"`    //24小时前价格
	HighPrice24h    float64 `json:"high_price_24h"`    //24小时最高价
	LowPrice24h     float64 `json:"low_price_24h"`     //24小时最低价
	Volume24h       float64 `json:"volume_24h"`        //24小时成交量
	Turnover24h     float64 `json:"turnover_24h"`      //24小时成交额
}
