package marketdata

type MarkPrice struct {
	Timestamp int64   `json:"timestamp" bson:"timestamp"`
	Exchange  string  `json:"exchange" bson:"exchange"`
	Symbol    string  `json:"symbol" bson:"symbol"`
	MarkPrice float64 `json:"mark_price" bson:"mark_price"`
}
