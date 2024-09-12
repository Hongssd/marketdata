package marketdata

type OptionTickerStats struct {
	Volume      float64 `json:"volume"`
	PriceChange float64 `json:"price_change"`
	Low         float64 `json:"low"`
	High        float64 `json:"high"`
}

type OptionTickerGreeks struct {
	Vega  float64 `json:"vega"`
	Theta float64 `json:"theta"`
	Rho   float64 `json:"rho"`
	Gamma float64 `json:"gamma"`
	Delta float64 `json:"delta"`
}

type OptionTicker struct {
	Exchange               string             `json:"exchange"`
	UnderlyingPrice        float64            `json:"underlying_price"`
	UnderlyingIndex        string             `json:"underlying_index"`
	Timestamp              int64              `json:"timestamp"`
	Stats                  OptionTickerStats  `json:"stats"`
	State                  string             `json:"state"`
	OpenInterest           float64            `json:"open_interest"`
	MinPrice               float64            `json:"min_price"`
	MaxPrice               float64            `json:"max_price"`
	MarkPrice              float64            `json:"mark_price"`
	MarkIv                 float64            `json:"mark_iv"`
	LastPrice              float64            `json:"last_price"`
	InterestRate           float64            `json:"interest_rate"`
	InstrumentName         string             `json:"instrument_name"`
	IndexPrice             float64            `json:"index_price"`
	Greeks                 OptionTickerGreeks `json:"greeks"`
	EstimatedDeliveryPrice float64            `json:"estimated_delivery_price"`
	BidIv                  float64            `json:"bid_iv"`
	BestBidPrice           float64            `json:"best_bid_price"`
	BestBidAmount          float64            `json:"best_bid_amount"`
	BestAskPrice           float64            `json:"best_ask_price"`
	BestAskAmount          float64            `json:"best_ask_amount"`
	AskIv                  float64            `json:"ask_iv"`
}

type OptionMarkPrice struct {
	InstType string  `json:"instType"` // 产品类型
	InstId   string  `json:"instId"`   // 产品ID
	MarkPx   float64 `json:"markPx"`   // 标记价格
	Ts       int64   `json:"ts"`       // 时间戳
}
