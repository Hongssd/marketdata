package marketdata

import (
	"github.com/Hongssd/mybinanceapi"
	"github.com/Hongssd/myokxapi"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var log = logrus.New()
var binance = mybinanceapi.MyBinance{}
var okx = myokxapi.MyOkx{}

func SetLogger(logger *logrus.Logger) {
	log = logger
}
