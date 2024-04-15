package marketdata

type Kline struct {
	Timestamp            int64   `json:"timestamp" bson:"timestamp"`
	Exchange             string  `json:"exchange" bson:"exchange"`                             //交易所
	AccountType          string  `json:"account_type" bson:"account_type"`                     //K线类型
	Symbol               string  `json:"symbol" bson:"symbol"`                                 //交易对
	Interval             string  `json:"interval" bson:"interval"`                             //K线间隔
	StartTime            int64   `json:"start_time" bson:"start_time" gorm:"primaryKey"`       //开盘时间
	Open                 float64 `json:"open" bson:"open"`                                     //开盘价
	High                 float64 `json:"high" bson:"high"`                                     //最高价
	Low                  float64 `json:"low" bson:"low"`                                       //最低价
	Close                float64 `json:"close" bson:"close"`                                   //收盘价
	Volume               float64 `json:"volume" bson:"volume"`                                 //成交量
	CloseTime            int64   `json:"close_time" bson:"close_time"`                         //收盘时间
	TransactionVolume    float64 `json:"transaction_volume" bson:"transaction_volume"`         //成交额
	TransactionNumber    int64   `json:"transaction_number" bson:"transaction_number"`         //成交笔数
	BuyTransactionVolume float64 `json:"buy_transaction_volume" bson:"buy_transaction_volume"` //主动买入成交量
	BuyTransactionAmount float64 `json:"buy_transaction_amount" bson:"buy_transaction_amount"` //主动买入成交额
	Confirm              bool    `json:"confirm" bson:"confirm"`                               //这根K线是否完结
}
