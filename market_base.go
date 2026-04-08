package marketdata

import (
	"github.com/Hongssd/myasterapi"
	"github.com/Hongssd/mybinanceapi"
	"github.com/Hongssd/mybitgetapi"
	"github.com/Hongssd/myokxapi"
	"github.com/Hongssd/mysunxapi"
	"github.com/Hongssd/myxcoinapi"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var log = logrus.New()
var binance = mybinanceapi.MyBinance{}
var bitget = mybitgetapi.MyBitget{}
var okx = myokxapi.MyOkx{}
var aster = myasterapi.MyAster{}
var sunx = mysunxapi.MySunx{}
var xcoin = myxcoinapi.MyXcoin{}

func SetLogger(logger *logrus.Logger) {
	log = logger
}
