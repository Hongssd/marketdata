package marketdata

import "testing"

func TestOkxDepthTargetTs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		resultTs         string
		serverTimeDelta  int64
		nowMs            int64
		want             int64
	}{
		{name: "exchange_behind_now", resultTs: "1000", serverTimeDelta: 10, nowMs: 2000, want: 1010},
		{name: "clamp_future_ts", resultTs: "5000", serverTimeDelta: 0, nowMs: 2000, want: 2000},
		{name: "empty_ts", resultTs: "", serverTimeDelta: 5, nowMs: 100, want: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := okxDepthTargetTs(tc.resultTs, tc.serverTimeDelta, tc.nowMs)
			if got != tc.want {
				t.Fatalf("okxDepthTargetTs=%d want %d", got, tc.want)
			}
		})
	}
}
