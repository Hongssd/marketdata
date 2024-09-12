package marketdata

type OkxOptionConfig struct {
	PerConnSubNum               int64 //每条链接订阅的数量
	PerSubMaxLen                int   //每条链接每次订阅的最大数量
	PerOptionMarkPriceSubNum    int64 //每条链接订阅的标记价格数量
	PerOptionMarkPriceSubMaxLen int   //每条链接每次订阅的标记价格最大数量
}
