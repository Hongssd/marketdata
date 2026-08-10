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

func TestBinanceFutureDepthBridges(t *testing.T) {
	const lastUpdateId int64 = 100

	tests := []struct {
		name   string
		upperU int64
		lowerU int64
		want   bool
	}{
		{name: "exact single update", upperU: 100, lowerU: 100, want: true},
		{name: "range covering", upperU: 90, lowerU: 110, want: true},
		{name: "before snapshot", upperU: 90, lowerU: 99, want: false},
		{name: "after snapshot gap", upperU: 101, lowerU: 101, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := binanceFutureDepthBridges(tt.upperU, tt.lowerU, lastUpdateId)
			if got != tt.want {
				t.Fatalf("binanceFutureDepthBridges(%d,%d,%d)=%v want %v",
					tt.upperU, tt.lowerU, lastUpdateId, got, tt.want)
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

func TestFindBinanceDepthBridgeIndex_FutureExact(t *testing.T) {
	lastUpdateId := int64(11197998430576)
	cacheList := []mybinanceapi.WsDepth{
		{UpperU: lastUpdateId - 20, LowerU: lastUpdateId - 10},
		{UpperU: lastUpdateId, LowerU: lastUpdateId},
		{UpperU: lastUpdateId + 1, LowerU: lastUpdateId + 5},
	}
	idx := findBinanceDepthBridgeIndex(BINANCE_FUTURE, cacheList, lastUpdateId)
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

func TestClassifyBinanceDepthBridgeMiss(t *testing.T) {
	lastUpdateId := int64(1000)

	t.Run("empty", func(t *testing.T) {
		got := classifyBinanceDepthBridgeMiss(BINANCE_FUTURE, nil, lastUpdateId)
		if got != binanceDepthBridgeMissEmptyCache {
			t.Fatalf("got %s want empty_cache", got)
		}
		if shouldClearDepthCacheOnBridgeMiss(got) {
			t.Fatal("empty cache must keep waiting without clear")
		}
		if !binanceDepthBridgeMissWorthWaiting(got) {
			t.Fatal("empty cache should wait")
		}
	})

	t.Run("cache behind snapshot", func(t *testing.T) {
		cache := []mybinanceapi.WsDepth{
			{UpperU: 900, LowerU: 910},
			{UpperU: 911, LowerU: 990},
		}
		got := classifyBinanceDepthBridgeMiss(BINANCE_FUTURE, cache, lastUpdateId)
		if got != binanceDepthBridgeMissCacheBehind {
			t.Fatalf("got %s want cache_behind", got)
		}
		if shouldClearDepthCacheOnBridgeMiss(got) {
			t.Fatal("cache_behind must keep cache for catch-up")
		}
		if !binanceDepthBridgeMissWorthWaiting(got) {
			t.Fatal("cache_behind should wait")
		}
	})

	t.Run("cache ahead after gap", func(t *testing.T) {
		// 快照落后于首个缓冲事件：min(U) > lastUpdateId，应保留缓存重取快照
		cache := []mybinanceapi.WsDepth{
			{UpperU: 1100, LowerU: 1100},
			{UpperU: 1101, LowerU: 1200},
		}
		got := classifyBinanceDepthBridgeMiss(BINANCE_FUTURE, cache, lastUpdateId)
		if got != binanceDepthBridgeMissCacheAhead {
			t.Fatalf("got %s want cache_ahead", got)
		}
		if shouldClearDepthCacheOnBridgeMiss(got) {
			t.Fatal("cache_ahead must keep cache and re-fetch snapshot")
		}
		if binanceDepthBridgeMissWorthWaiting(got) {
			t.Fatal("cache_ahead should not wait on same snapshot")
		}
	})

	t.Run("middle hole no covering event", func(t *testing.T) {
		cache := []mybinanceapi.WsDepth{
			{UpperU: 900, LowerU: 950},
			{UpperU: 1100, LowerU: 1150},
		}
		got := classifyBinanceDepthBridgeMiss(BINANCE_FUTURE, cache, lastUpdateId)
		if got != binanceDepthBridgeMissNoCovering {
			t.Fatalf("got %s want no_covering", got)
		}
		if !shouldClearDepthCacheOnBridgeMiss(got) {
			t.Fatal("no_covering must clear cache")
		}
	})

	t.Run("bridging succeeds", func(t *testing.T) {
		cache := []mybinanceapi.WsDepth{
			{UpperU: 990, LowerU: 1010},
		}
		got := classifyBinanceDepthBridgeMiss(BINANCE_FUTURE, cache, lastUpdateId)
		if got != binanceDepthBridgeMissNone {
			t.Fatalf("got %s want none", got)
		}
	})
}

func TestShouldClearDepthCacheOnBridgeMiss_SpotAhead(t *testing.T) {
	lastUpdateId := int64(100)
	// spot target = 101；缓存从 102 开始
	cache := []mybinanceapi.WsDepth{
		{UpperU: 102, LowerU: 102},
		{UpperU: 103, LowerU: 110},
	}
	got := classifyBinanceDepthBridgeMiss(BINANCE_SPOT, cache, lastUpdateId)
	if got != binanceDepthBridgeMissCacheAhead {
		t.Fatalf("got %s want cache_ahead", got)
	}
}
