package marketdata

import (
	"github.com/Hongssd/mybinanceapi"
	"github.com/Hongssd/mygateapi"
	"github.com/shopspring/decimal"
)

type Depth struct {
	UId         int64        `json:"u_id"`
	PreUId      int64        `json:"pre_u_id"`
	Exchange    string       `json:"exchange"`
	AccountType string       `json:"account_type"`
	Symbol      string       `json:"symbol"`
	Timestamp   int64        `json:"timestamp"`
	Bids        []PriceLevel `json:"bids"`
	Asks        []PriceLevel `json:"asks"`
}

func (d *Depth) WeightedAvgPrice(level int) float64 {
	var sumPrice float64
	var sumQuantity float64

	min := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}

	for i := 0; i < min(level, len(d.Bids)); i++ {
		sumPrice += d.Bids[i].Price * d.Bids[i].Quantity
		sumQuantity += d.Bids[i].Quantity
	}
	return sumPrice / sumQuantity
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

type SortGateWsOrderBookSlice []mygateapi.WsOrderBook

func (s SortGateWsOrderBookSlice) Len() int {
	return len(s)
}

func (s SortGateWsOrderBookSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortGateWsOrderBookSlice) Less(i, j int) bool {
	return s[i].LastId < s[j].LastId
}
