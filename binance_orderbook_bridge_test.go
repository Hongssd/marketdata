package marketdata

import (
	"testing"

	"github.com/Hongssd/mybinanceapi"
)

func TestBinanceSpotDepthBridges(t *testing.T) {
	const lastUpdateId int64 = 100

	tests := []struct {
		name    string
		upperU  int64
		lowerU  int64
		want    bool
		comment string
	}{
		{
			name:    "single update at lastUpdateId+1",
			upperU:  101,
			lowerU:  101,
			want:    true,
			comment: "官方 U <= lastUpdateId+1 <= u；旧条件 UpperU < lastUpdateId+1 会误判失败",
		},
		{
			name:   "range covering lastUpdateId+1",
			upperU: 100,
			lowerU: 105,
			want:   true,
		},
		{
			name:   "U equals lastUpdateId+1 lower beyond",
			upperU: 101,
			lowerU: 110,
			want:   true,
		},
		{
			name:   "entirely before snapshot",
			upperU: 90,
			lowerU: 100,
			want:   false,
		},
		{
			name:   "gap after snapshot",
			upperU: 102,
			lowerU: 102,
			want:   false,
		},
		{
			name:   "old buggy equality case must pass",
			upperU: 101,
			lowerU: 101,
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := binanceSpotDepthBridges(tt.upperU, tt.lowerU, lastUpdateId)
			if got != tt.want {
				t.Fatalf("binanceSpotDepthBridges(%d,%d,%d)=%v want %v (%s)",
					tt.upperU, tt.lowerU, lastUpdateId, got, tt.want, tt.comment)
			}
		})
	}
}

func TestFindBinanceDepthBridgeIndex_SpotSingleUpdate(t *testing.T) {
	lastUpdateId := int64(98026711703)
	cacheList := []mybinanceapi.WsDepth{
		{UpperU: lastUpdateId - 10, LowerU: lastUpdateId - 5},
		{UpperU: lastUpdateId + 1, LowerU: lastUpdateId + 1},
		{UpperU: lastUpdateId + 2, LowerU: lastUpdateId + 2},
	}

	idx := findBinanceDepthBridgeIndex(BINANCE_SPOT, cacheList, lastUpdateId)
	if idx != 1 {
		t.Fatalf("bridge index=%d want 1", idx)
	}
}

func TestFindBinanceDepthBridgeIndex_SpotOldConditionWouldMiss(t *testing.T) {
	lastUpdateId := int64(100)
	// 旧条件: UpperU < lastUpdateId+1 && LowerU >= lastUpdateId+1
	oldBuggy := func(upperU, lowerU, lu int64) bool {
		return upperU < lu+1 && lowerU >= lu+1
	}
	upperU, lowerU := int64(101), int64(101)
	if oldBuggy(upperU, lowerU, lastUpdateId) {
		t.Fatal("expected old buggy condition to miss U=u=lastUpdateId+1")
	}
	if !binanceSpotDepthBridges(upperU, lowerU, lastUpdateId) {
		t.Fatal("fixed condition must accept U=u=lastUpdateId+1")
	}
}
