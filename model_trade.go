package marketdata

type Trade struct {
	AId         string  `json:"a_id" bson:"a_id"`                // 逐笔交易ID
	AccountType string  `json:"accountType" bson:"account_type"` //类型 现货:spot 币合约:swap u合约:future
	Exchange    string  `json:"exchange" bson:"exchange"`        //交易所
	Symbol      string  `json:"symbol" bson:"symbol"`            //交易对
	Timestamp   int64   `json:"timestamp" bson:"timestamp"`      //事件时间
	Price       float64 `json:"price" bson:"price"`              //成交价
	Quantity    float64 `json:"quantity" bson:"quantity"`        //成交量
	TradeTime   int64   `json:"tradeTime" bson:"trade_time"`     //成交时间
	IsBuyer     bool    `json:"isBuyer" bson:"is_buyer"`         //是否为买方
	IsMarket    bool    `json:"isMarket" bson:"is_market"`       //买方是否做市方
}
