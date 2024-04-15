package marketdata

import "time"

func BinanceGetServerTimeDelta(accountType BinanceAccountType) (int64, error) {
	switch accountType {
	case BINANCE_SPOT:
		res, err := binance.NewSpotRestClient("", "").NewServerTime().Do()
		if err != nil {
			return 0, err
		}
		return time.Now().UnixMilli() - res.ServerTime, nil
	case BINANCE_FUTURE:
		res, err := binance.NewFutureRestClient("", "").NewServerTime().Do()
		if err != nil {
			return 0, err
		}
		return time.Now().UnixMilli() - res.ServerTime, nil
	case BINANCE_SWAP:
		res, err := binance.NewSwapRestClient("", "").NewServerTime().Do()
		if err != nil {
			return 0, err
		}
		return time.Now().UnixMilli() - res.ServerTime, nil
	default:
		return 0, ErrorAccountType
	}

	return 0, nil
}
