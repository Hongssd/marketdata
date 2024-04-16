package marketdata

type AggTrade struct {
	AId         int64   `json:"a_id" bson:"a_id"`                // 归集交易ID
	AccountType string  `json:"accountType" bson:"account_type"` //类型 现货:spot 币合约:swap u合约:future
	Exchange    string  `json:"exchange" bson:"exchange"`        //交易所
	Symbol      string  `json:"symbol" bson:"symbol"`            //交易对
	Timestamp   int64   `json:"timestamp" bson:"timestamp"`      //事件时间
	Price       float64 `json:"price" bson:"price"`              //成交价
	Quantity    float64 `json:"quantity" bson:"quantity"`        //成交量
	First       int64   `json:"first" bson:"first"`              //被归集的首次交易ID
	Last        int64   `json:"last" bson:"last"`                //被归集的末次交易ID
	TradeTime   int64   `json:"tradeTime" bson:"trade_time"`     //成交时间
	IsMarket    bool    `json:"isMarket" bson:"is_market"`       //买方是否做市方
}
