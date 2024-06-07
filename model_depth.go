package marketdata

import (
	"github.com/Hongssd/mybinanceapi"
	"github.com/shopspring/decimal"
)

type Depth struct {
	Uid         int64        `json:"u_id"`
	Exchange    string       `json:"exchange"`
	AccountType string       `json:"account_type"`
	Symbol      string       `json:"symbol"`
	Timestamp   int64        `json:"timestamp"`
	Bids        []PriceLevel `json:"bids"`
	Asks        []PriceLevel `json:"asks"`
}

type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

func (p *PriceLevel) Float() (float64, float64) {
	return p.Price, p.Quantity
}

func (p *PriceLevel) Decimal() (decimal.Decimal, decimal.Decimal) {
	return decimal.NewFromFloat(p.Price), decimal.NewFromFloat(p.Quantity)
}

func (p *PriceLevel) String() (string, string) {
	return p.StringFixed(8)
}

func (p *PriceLevel) StringFixed(fixed int32) (string, string) {
	return decimal.NewFromFloat(p.Price).StringFixed(fixed), decimal.NewFromFloat(p.Quantity).StringFixed(fixed)
}

type SortBinanceWsDepthSlice []mybinanceapi.WsDepth

func (s SortBinanceWsDepthSlice) Len() int {
	return len(s)
}

func (s SortBinanceWsDepthSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortBinanceWsDepthSlice) Less(i, j int) bool {
	return s[i].LowerU < s[j].LowerU
}
