package marketdata

import (
	"errors"
	"sync"
	"testing"
	"time"

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

func TestDecideBinanceDepthBridgeMissAction(t *testing.T) {
	tests := []struct {
		reason binanceDepthBridgeMissReason
		want   binanceDepthMissAction
	}{
		{binanceDepthBridgeMissEmptyCache, binanceDepthMissWaitSameSnapshot},
		{binanceDepthBridgeMissCacheBehind, binanceDepthMissWaitSameSnapshot},
		{binanceDepthBridgeMissCacheAhead, binanceDepthMissRefetchKeepCache},
		{binanceDepthBridgeMissNoCovering, binanceDepthMissRestartClearCache},
		{binanceDepthBridgeMissNone, binanceDepthMissRestartClearCache},
	}
	for _, tt := range tests {
		t.Run(string(tt.reason), func(t *testing.T) {
			got := decideBinanceDepthBridgeMissAction(tt.reason)
			if got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}

func TestBinanceDepthBackoffDuration(t *testing.T) {
	min := 200 * time.Millisecond
	max := 2 * time.Second
	if got := binanceDepthBackoffDuration(0, min, max); got != min {
		t.Fatalf("attempt0=%s want %s", got, min)
	}
	if got := binanceDepthBackoffDuration(1, min, max); got != 400*time.Millisecond {
		t.Fatalf("attempt1=%s want 400ms", got)
	}
	if got := binanceDepthBackoffDuration(2, min, max); got != 800*time.Millisecond {
		t.Fatalf("attempt2=%s want 800ms", got)
	}
	if got := binanceDepthBackoffDuration(10, min, max); got != max {
		t.Fatalf("attempt10=%s want max %s", got, max)
	}
}

func TestBinanceDepthCacheNeedsRestart(t *testing.T) {
	if binanceDepthCacheNeedsRestart(4095, 4096) {
		t.Fatal("4095 should not restart")
	}
	if !binanceDepthCacheNeedsRestart(4096, 4096) {
		t.Fatal("4096 should restart")
	}
	if binanceDepthCacheNeedsRestart(100, 0) {
		t.Fatal("cap 0 disables restart")
	}
}

func TestBinanceDepthWaitStallCount(t *testing.T) {
	tests := []struct {
		name       string
		prevLen    int
		curLen     int
		prevMaxU   int64
		curMaxU    int64
		stallCount int
		want       int
	}{
		{name: "first sample resets", prevLen: -1, curLen: 95, prevMaxU: 0, curMaxU: 100, stallCount: 0, want: 0},
		{name: "frozen cache increments", prevLen: 95, curLen: 95, prevMaxU: 11288324596693, curMaxU: 11288324596693, stallCount: 3, want: 4},
		{name: "len growth resets", prevLen: 95, curLen: 96, prevMaxU: 100, curMaxU: 100, stallCount: 10, want: 0},
		{name: "maxU growth resets", prevLen: 95, curLen: 95, prevMaxU: 100, curMaxU: 101, stallCount: 10, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := binanceDepthWaitStallCount(tt.prevLen, tt.curLen, tt.prevMaxU, tt.curMaxU, tt.stallCount)
			if got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}

func TestDecideBinanceDepthSameSnapshotWait(t *testing.T) {
	const stallLimit = 40
	const timeoutWait = 200
	tests := []struct {
		name       string
		waitCount  int
		stallCount int
		maxWait    int
		want       binanceDepthSameSnapshotWaitDecision
	}{
		{name: "keep waiting early", waitCount: 1, stallCount: 1, maxWait: timeoutWait, want: binanceDepthKeepWaiting},
		{name: "stall limit frozen cache", waitCount: 40, stallCount: 40, maxWait: timeoutWait, want: binanceDepthResubKeepSnapshot},
		{name: "timeout even if progressing", waitCount: 200, stallCount: 0, maxWait: timeoutWait, want: binanceDepthAbandonWaitTimeout},
		{name: "stall wins over timeout", waitCount: 200, stallCount: 40, maxWait: timeoutWait, want: binanceDepthResubKeepSnapshot},
		{name: "just below stall", waitCount: 39, stallCount: 39, maxWait: timeoutWait, want: binanceDepthKeepWaiting},
		{name: "just below timeout", waitCount: 199, stallCount: 0, maxWait: timeoutWait, want: binanceDepthKeepWaiting},
		{name: "production maxWait 0 never times out", waitCount: 200, stallCount: 0, maxWait: 0, want: binanceDepthKeepWaiting},
		{name: "production stall still resub", waitCount: 40, stallCount: 40, maxWait: 0, want: binanceDepthResubKeepSnapshot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideBinanceDepthSameSnapshotWait(tt.waitCount, tt.stallCount, tt.maxWait, stallLimit)
			if got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}

func TestDecideBinanceDepthSameSnapshotWait_ProductionFrozenCacheBehind(t *testing.T) {
	// 生产 BTCUSDT: cacheLen/maxLowerU 冻住，waitCount 到百万。2s 冻结即应放弃同快照空等。
	stall := 0
	const frozenLen = 95
	const frozenMaxU int64 = 11288324596693
	for i := 0; i < binanceDepthSameSnapshotStallLimit; i++ {
		stall = binanceDepthWaitStallCount(frozenLen, frozenLen, frozenMaxU, frozenMaxU, stall)
	}
	got := decideBinanceDepthSameSnapshotWait(binanceDepthSameSnapshotStallLimit, stall, binanceDepthSameSnapshotMaxWait, binanceDepthSameSnapshotStallLimit)
	if got != binanceDepthResubKeepSnapshot {
		t.Fatalf("frozen cache_behind got %d want resub keep snapshot", got)
	}
}

func TestBinanceDepthEventFollows_Future(t *testing.T) {
	last := int64(100)
	if got := binanceDepthEventFollows(BINANCE_FUTURE, last, mybinanceapi.WsDepth{UpperU: 90, LowerU: 99, PreU: 89}); got != binanceDepthFollowSkip {
		t.Fatalf("stale got %d", got)
	}
	if got := binanceDepthEventFollows(BINANCE_FUTURE, last, mybinanceapi.WsDepth{UpperU: 101, LowerU: 105, PreU: 100}); got != binanceDepthFollowApply {
		t.Fatalf("pu match got %d", got)
	}
	if got := binanceDepthEventFollows(BINANCE_FUTURE, last, mybinanceapi.WsDepth{UpperU: 101, LowerU: 105, PreU: 0}); got != binanceDepthFollowApply {
		t.Fatalf("pre=0 fallback UpperU-1 got %d", got)
	}
	if got := binanceDepthEventFollows(BINANCE_FUTURE, last, mybinanceapi.WsDepth{UpperU: 110, LowerU: 120, PreU: 109}); got != binanceDepthFollowHole {
		t.Fatalf("gap got %d", got)
	}
}

func TestApplyBinanceDepthCacheContiguous_FutureHole(t *testing.T) {
	last := int64(100)
	cache := []mybinanceapi.WsDepth{
		{UpperU: 90, LowerU: 100, PreU: 89},
		{UpperU: 101, LowerU: 110, PreU: 100},
		{UpperU: 200, LowerU: 210, PreU: 199},
	}
	var applied []int64
	gotLast, hole, err := applyBinanceDepthCacheContiguous(BINANCE_FUTURE, last, cache, 0, func(v mybinanceapi.WsDepth) error {
		applied = append(applied, v.LowerU)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hole {
		t.Fatal("expected hole after gap")
	}
	if gotLast != 110 {
		t.Fatalf("gotLast=%d want 110", gotLast)
	}
	if len(applied) != 2 || applied[0] != 100 || applied[1] != 110 {
		t.Fatalf("applied=%v", applied)
	}
}

func TestApplyBinanceDepthCacheContiguous_FutureOk(t *testing.T) {
	last := int64(100)
	cache := []mybinanceapi.WsDepth{
		{UpperU: 95, LowerU: 100, PreU: 94},
		{UpperU: 101, LowerU: 103, PreU: 100},
		{UpperU: 104, LowerU: 104, PreU: 103},
	}
	var applied []int64
	gotLast, hole, err := applyBinanceDepthCacheContiguous(BINANCE_FUTURE, last, cache, 0, func(v mybinanceapi.WsDepth) error {
		applied = append(applied, v.LowerU)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if hole {
		t.Fatal("did not expect hole")
	}
	if gotLast != 104 {
		t.Fatalf("gotLast=%d want 104", gotLast)
	}
	if len(applied) != 3 {
		t.Fatalf("applied=%v", applied)
	}
}

func newTestBinanceOrderBookBase() *binanceOrderBookBase {
	return &binanceOrderBookBase{
		BinanceWsClientBase: BinanceWsClientBase{
			WsClientListMap: GetPointer(NewMySyncMap[*mybinanceapi.WsStreamClient, *int64]()),
		},
		OrderBookCacheMap:         GetPointer(NewMySyncMap[string, *MySyncMap[int64, *mybinanceapi.WsDepth]]()),
		OrderBookReadyUpdateIdMap: GetPointer(NewMySyncMap[string, int64]()),
		OrderBookLastUpdateIdMap:  GetPointer(NewMySyncMap[string, int64]()),
		WsClientMap:               GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]()),
		SubMap:                    GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		depthResubInFlight:        GetPointer(NewMySyncMap[string, bool]()),
		depthResubAttempt:         GetPointer(NewMySyncMap[string, int]()),
	}
}

func TestBinanceDepthResubBackoffAfterAttempt(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 0},
		{attempt: 1, want: 0},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 8 * time.Second},
		{attempt: 20, want: 60 * time.Second},
	}
	for _, tt := range tests {
		got := binanceDepthResubBackoffAfterAttempt(tt.attempt)
		if got != tt.want {
			t.Fatalf("attempt=%d got %s want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestBinanceDepthWsClientStillUsed(t *testing.T) {
	shared := &mybinanceapi.WsStreamClient{}
	other := &mybinanceapi.WsStreamClient{}
	clients := GetPointer(NewMySyncMap[string, *mybinanceapi.WsStreamClient]())
	clients.Store("BTCUSDT", shared)
	clients.Store("ETHUSDT", shared)
	clients.Store("SOLUSDT", other)

	if !binanceDepthWsClientStillUsed(clients, shared, binanceDepthSymbolSet([]string{"BTCUSDT"})) {
		t.Fatal("ETHUSDT still on shared client")
	}
	if binanceDepthWsClientStillUsed(clients, shared, binanceDepthSymbolSet([]string{"BTCUSDT", "ETHUSDT"})) {
		t.Fatal("excluded all symbols on shared client")
	}
	if binanceDepthWsClientStillUsed(clients, shared, nil) == false {
		t.Fatal("nil exclude should see BTC/ETH")
	}
	if binanceDepthWsClientStillUsed(nil, shared, nil) {
		t.Fatal("nil map")
	}
	if binanceDepthWsClientStillUsed(clients, nil, nil) {
		t.Fatal("nil client")
	}
	if binanceDepthWsClientStillUsed(clients, &mybinanceapi.WsStreamClient{}, nil) {
		t.Fatal("unknown client")
	}
}

func TestUnsubscribeBinanceDepthSubWithTimeout(t *testing.T) {
	if err := unsubscribeBinanceDepthSubWithTimeout(nil, time.Second); err != nil {
		t.Fatalf("nil unsub: %v", err)
	}
	if err := unsubscribeBinanceDepthSubWithTimeout(func() error { return nil }, time.Second); err != nil {
		t.Fatalf("ok unsub: %v", err)
	}
	want := errors.New("unsub fail")
	if err := unsubscribeBinanceDepthSubWithTimeout(func() error { return want }, time.Second); !errors.Is(err, want) {
		t.Fatalf("got %v want %v", err, want)
	}

	release := make(chan struct{})
	err := unsubscribeBinanceDepthSubWithTimeout(func() error {
		<-release
		return nil
	}, 20*time.Millisecond)
	if !errors.Is(err, errBinanceDepthUnsubscribeTimeout) {
		t.Fatalf("got %v want timeout", err)
	}
	close(release)
}

func TestClaimDepthResubSymbols_InFlightAndBackoff(t *testing.T) {
	b := newTestBinanceOrderBookBase()
	pending, backoff := b.claimDepthResubSymbols([]string{"BTCUSDT", "ETHUSDT"})
	if len(pending) != 2 {
		t.Fatalf("pending=%v", pending)
	}
	if backoff != 0 {
		t.Fatalf("first claim backoff=%s want 0", backoff)
	}
	pending2, _ := b.claimDepthResubSymbols([]string{"BTCUSDT", "ETHUSDT"})
	if len(pending2) != 0 {
		t.Fatalf("in-flight should skip, got %v", pending2)
	}

	b.releaseDepthResubInFlight([]string{"BTCUSDT"})
	pending3, backoff3 := b.claimDepthResubSymbols([]string{"BTCUSDT", "ETHUSDT"})
	if len(pending3) != 1 || pending3[0] != "BTCUSDT" {
		t.Fatalf("pending3=%v", pending3)
	}
	if backoff3 != 2*time.Second {
		t.Fatalf("second claim backoff=%s want 2s", backoff3)
	}
	if attempt, _ := b.depthResubAttempt.Load("BTCUSDT"); attempt != 2 {
		t.Fatalf("BTCUSDT attempt=%d want 2", attempt)
	}
	if attempt, _ := b.depthResubAttempt.Load("ETHUSDT"); attempt != 1 {
		t.Fatalf("ETHUSDT attempt=%d want 1", attempt)
	}
}

func TestSymbolsOnSameDepthSub(t *testing.T) {
	b := newTestBinanceOrderBookBase()
	shared := &mybinanceapi.Subscription[mybinanceapi.WsDepth]{}
	alone := &mybinanceapi.Subscription[mybinanceapi.WsDepth]{}
	b.SubMap.Store("BTCUSDT", shared)
	b.SubMap.Store("ETHUSDT", shared)
	b.SubMap.Store("SOLUSDT", alone)

	got := b.symbolsOnSameDepthSub("BTCUSDT")
	if len(got) != 2 {
		t.Fatalf("shared got=%v", got)
	}
	seen := map[string]bool{}
	for _, s := range got {
		seen[s] = true
	}
	if !seen["BTCUSDT"] || !seen["ETHUSDT"] {
		t.Fatalf("shared got=%v", got)
	}
	if only := b.symbolsOnSameDepthSub("SOLUSDT"); len(only) != 1 || only[0] != "SOLUSDT" {
		t.Fatalf("alone got=%v", only)
	}
	if missing := b.symbolsOnSameDepthSub("XRPUSDT"); len(missing) != 1 || missing[0] != "XRPUSDT" {
		t.Fatalf("missing got=%v", missing)
	}
}

func TestBinanceDepthSubStillActive(t *testing.T) {
	b := newTestBinanceOrderBookBase()
	oldSub := &mybinanceapi.Subscription[mybinanceapi.WsDepth]{}
	newSub := &mybinanceapi.Subscription[mybinanceapi.WsDepth]{}
	b.SubMap.Store("BTCUSDT", oldSub)
	if !b.binanceDepthSubStillActive(oldSub, []string{"BTCUSDT"}) {
		t.Fatal("active sub should resub")
	}
	b.SubMap.Store("BTCUSDT", newSub)
	if b.binanceDepthSubStillActive(oldSub, []string{"BTCUSDT"}) {
		t.Fatal("replaced sub should skip")
	}
	b.SubMap.Delete("BTCUSDT")
	if b.binanceDepthSubStillActive(newSub, []string{"BTCUSDT"}) {
		t.Fatal("detached sub should skip")
	}
}

func TestInvalidateBinanceDepthReadyKeepSnapshot(t *testing.T) {
	b := newTestBinanceOrderBookBase()
	b.OrderBookReadyUpdateIdMap.Store("BTCUSDT", int64(1))
	b.OrderBookLastUpdateIdMap.Store("BTCUSDT", int64(99))
	cache := NewMySyncMap[int64, *mybinanceapi.WsDepth]()
	cache.Store(1, &mybinanceapi.WsDepth{LowerU: 1})
	b.OrderBookCacheMap.Store("BTCUSDT", &cache)

	b.invalidateBinanceDepthReadyKeepSnapshot("BTCUSDT")
	if _, ok := b.OrderBookReadyUpdateIdMap.Load("BTCUSDT"); ok {
		t.Fatal("ready should be cleared")
	}
	if _, ok := b.OrderBookCacheMap.Load("BTCUSDT"); ok {
		t.Fatal("cache should be cleared")
	}
	if id, ok := b.OrderBookLastUpdateIdMap.Load("BTCUSDT"); !ok || id != 99 {
		t.Fatalf("lastUpdateId=%d ok=%v want 99", id, ok)
	}
}

func TestDetachBinanceDepthSymbols_KeepsSibling(t *testing.T) {
	b := newTestBinanceOrderBookBase()
	client := &mybinanceapi.WsStreamClient{}
	count := int64(2)
	b.WsClientListMap.Store(client, &count)
	b.WsClientMap.Store("BTCUSDT", client)
	b.WsClientMap.Store("ETHUSDT", client)
	b.SubMap.Store("BTCUSDT", &mybinanceapi.Subscription[mybinanceapi.WsDepth]{})
	b.SubMap.Store("ETHUSDT", &mybinanceapi.Subscription[mybinanceapi.WsDepth]{})

	got := b.detachBinanceDepthSymbols([]string{"BTCUSDT"})
	if len(got) != 1 || got[0] != client {
		t.Fatalf("clients=%v", got)
	}
	if _, ok := b.WsClientMap.Load("BTCUSDT"); ok {
		t.Fatal("BTCUSDT should be detached")
	}
	if c, ok := b.WsClientMap.Load("ETHUSDT"); !ok || c != client {
		t.Fatal("ETHUSDT should remain")
	}
	if _, ok := b.SubMap.Load("BTCUSDT"); ok {
		t.Fatal("BTCUSDT sub should be detached")
	}
	if count != 1 {
		t.Fatalf("count=%d want 1", count)
	}
}

func TestSaveBinanceDepthCache_StoresCopy(t *testing.T) {
	b := newTestBinanceOrderBookBase()
	result := mybinanceapi.WsDepth{Symbol: "BTCUSDT", LowerU: 10, UpperU: 9}
	b.saveBinanceDepthCache(result)
	result.LowerU = 99
	cache, ok := b.OrderBookCacheMap.Load("BTCUSDT")
	if !ok {
		t.Fatal("missing cache")
	}
	got, ok := cache.Load(int64(10))
	if !ok || got == nil || got.LowerU != 10 || got.UpperU != 9 {
		t.Fatalf("stored=%v ok=%v", got, ok)
	}
}

func TestBinanceDepthInitLimit(t *testing.T) {
	tests := []struct {
		name string
		size int
		want int
	}{
		{name: "zero defaults to 100", size: 0, want: 100},
		{name: "negative defaults to 100", size: -1, want: 100},
		{name: "explicit 100", size: 100, want: 100},
		{name: "explicit 1000 still honored", size: 1000, want: 1000},
		{name: "explicit 50", size: 50, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := binanceDepthInitLimit(tt.size); got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}

func TestDecideBinanceDepthSameSnapshotWait_GrowingCacheNoRest(t *testing.T) {
	// 生产 maxWait=0：缓存仍在增长时即使 waitCount 很大也不放弃同快照。
	got := decideBinanceDepthSameSnapshotWait(1_000_000, 0, binanceDepthSameSnapshotMaxWait, binanceDepthSameSnapshotStallLimit)
	if got != binanceDepthKeepWaiting {
		t.Fatalf("got %d want keep waiting", got)
	}
}

func TestDepthInitAborted(t *testing.T) {
	b := &binanceOrderBookBase{
		depthInitEpoch: GetPointer(NewMySyncMap[string, int64]()),
	}
	if b.depthInitAborted("BTCUSDT", 0) {
		t.Fatal("epoch 0 should not abort")
	}
	b.bumpDepthInitEpoch([]string{"BTCUSDT", ""})
	if !b.depthInitAborted("BTCUSDT", 0) {
		t.Fatal("bumped epoch should abort old init")
	}
	if b.depthInitAborted("BTCUSDT", 1) {
		t.Fatal("current epoch should not abort")
	}
	b.closed.Store(true)
	if !b.depthInitAborted("BTCUSDT", 1) {
		t.Fatal("closed should abort")
	}
	empty := &binanceOrderBookBase{}
	if empty.depthInitAborted("BTCUSDT", 0) {
		t.Fatal("nil epoch map with closed=false should not abort epoch 0")
	}
}

func TestNilOrderBookDepthInitAborted(t *testing.T) {
	var b *binanceOrderBookBase
	if !b.depthInitAborted("BTCUSDT", 0) {
		t.Fatal("nil base should abort")
	}
}

func TestReSubscribeOrderBook_RejectsEmptyAndBadAccount(t *testing.T) {
	ob := &BinanceOrderBook{}
	ob.FutureOrderBook = ob.newBinanceOrderBookBase(BinanceOrderBookConfigBase{})
	ob.FutureOrderBook.AccountType = BINANCE_FUTURE
	if err := ob.ReSubscribeOrderBook(BINANCE_FUTURE, "  "); err == nil {
		t.Fatal("expected empty symbol error")
	}
	if err := ob.ReSubscribeOrderBook("UNKNOWN", "BTCUSDT"); err == nil {
		t.Fatal("expected account type error")
	}
	ob.FutureOrderBook.closed.Store(true)
	if err := ob.ReSubscribeOrderBook(BINANCE_FUTURE, "BTCUSDT"); err == nil {
		t.Fatal("expected closed error")
	}
}

func TestWaitDepthInitIdle_NoMutexNoPanic(t *testing.T) {
	b := &binanceOrderBookBase{
		IsInitActionMu: GetPointer(NewMySyncMap[string, *sync.Mutex]()),
	}
	b.waitDepthInitIdle([]string{"BTCUSDT", ""})
	mu := &sync.Mutex{}
	b.IsInitActionMu.Store("ETHUSDT", mu)
	b.waitDepthInitIdle([]string{"ETHUSDT"})
}

func TestWaitDepthInitIdle_PerSymbolTimeout(t *testing.T) {
	b := &binanceOrderBookBase{
		IsInitActionMu: GetPointer(NewMySyncMap[string, *sync.Mutex]()),
	}
	mu1, mu2 := &sync.Mutex{}, &sync.Mutex{}
	mu1.Lock()
	mu2.Lock()
	b.IsInitActionMu.Store("BTCUSDT", mu1)
	b.IsInitActionMu.Store("ETHUSDT", mu2)
	start := time.Now()
	b.waitDepthInitIdleFor([]string{"BTCUSDT", "ETHUSDT"}, 80*time.Millisecond)
	elapsed := time.Since(start)
	mu1.Unlock()
	mu2.Unlock()
	if elapsed < 150*time.Millisecond {
		t.Fatalf("per-symbol timeout too short elapsed=%s", elapsed)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("per-symbol timeout too long elapsed=%s", elapsed)
	}
}

func TestOrderBookClose_NilSafe(t *testing.T) {
	var base *binanceOrderBookBase
	base.Close()
	var ob *BinanceOrderBook
	ob.Close()
}

func TestBinanceDepthAnyResubInFlight(t *testing.T) {
	if binanceDepthAnyResubInFlight(nil, []string{"BTCUSDT"}) {
		t.Fatal("nil map should not be in-flight")
	}
	m := GetPointer(NewMySyncMap[string, bool]())
	m.Store("ETHUSDT", true)
	if binanceDepthAnyResubInFlight(m, []string{"BTCUSDT"}) {
		t.Fatal("other symbol in-flight should not match")
	}
	if !binanceDepthAnyResubInFlight(m, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Fatal("sibling in-flight should match")
	}
}

func TestBinanceDepthGroupRecentlyRestarted(t *testing.T) {
	last := GetPointer(NewMySyncMap[string, int64]())
	now := int64(1_000_000)
	stampBinanceDepthGroupRestarted(last, []string{"BTCUSDT", "ETHUSDT"}, now)
	if !binanceDepthGroupRecentlyRestarted(last, []string{"ETHUSDT"}, now+1000, 3*time.Second) {
		t.Fatal("within debounce window")
	}
	if binanceDepthGroupRecentlyRestarted(last, []string{"ETHUSDT"}, now+4000, 3*time.Second) {
		t.Fatal("debounce elapsed")
	}
	if binanceDepthGroupRecentlyRestarted(nil, []string{"ETHUSDT"}, now, 3*time.Second) {
		t.Fatal("nil map")
	}
}

func TestRestartLocked_SkipInFlightNoBump(t *testing.T) {
	b := &binanceOrderBookBase{
		SubMap:                GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		depthInitEpoch:        GetPointer(NewMySyncMap[string, int64]()),
		depthResubInFlight:    GetPointer(NewMySyncMap[string, bool]()),
		depthLastRestartMilli: GetPointer(NewMySyncMap[string, int64]()),
	}
	b.depthInitEpoch.Store("BTCUSDT", 5)
	b.depthResubInFlight.Store("BTCUSDT", true)
	b.restartBinanceDepthStreamLocked("BTCUSDT", false)
	if got := b.currentDepthInitEpoch("BTCUSDT"); got != 5 {
		t.Fatalf("in-flight restart must not bump epoch, got %d", got)
	}
}

func TestRestartLocked_SkipDebounceNoBump(t *testing.T) {
	b := &binanceOrderBookBase{
		SubMap:                GetPointer(NewMySyncMap[string, *mybinanceapi.Subscription[mybinanceapi.WsDepth]]()),
		depthInitEpoch:        GetPointer(NewMySyncMap[string, int64]()),
		depthResubInFlight:    GetPointer(NewMySyncMap[string, bool]()),
		depthLastRestartMilli: GetPointer(NewMySyncMap[string, int64]()),
	}
	b.depthInitEpoch.Store("BTCUSDT", 5)
	stampBinanceDepthGroupRestarted(b.depthLastRestartMilli, []string{"BTCUSDT"}, time.Now().UnixMilli())
	b.restartBinanceDepthStreamLocked("BTCUSDT", false)
	if got := b.currentDepthInitEpoch("BTCUSDT"); got != 5 {
		t.Fatalf("debounced restart must not bump epoch, got %d", got)
	}
}

func TestReSubscribeOrderBook_NilReceiver(t *testing.T) {
	var ob *BinanceOrderBook
	if err := ob.ReSubscribeOrderBook(BINANCE_FUTURE, "BTCUSDT"); err == nil {
		t.Fatal("expected nil receiver error")
	}
	ob = &BinanceOrderBook{}
	if err := ob.ReSubscribeOrderBook(BINANCE_FUTURE, "BTCUSDT"); err == nil {
		t.Fatal("expected nil future book error")
	}
}
