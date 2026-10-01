package main

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxFailureDetails = 20

type stats struct {
	out   io.Writer
	start time.Time

	events    atomic.Int64
	commits   atomic.Int64
	commitsOK atomic.Int64
	syncs     atomic.Int64
	syncsOK   atomic.Int64
	identity  atomic.Int64
	account   atomic.Int64
	info      atomic.Int64
	unknown   atomic.Int64

	mu         sync.Mutex
	fails      map[string]int64
	totalFails int64
	printed    int
	lags       []time.Duration // since last report
	allLags    lagHist         // whole run, for the summary

	lastReport time.Time
	lastEvents int64
}

func newStats(out io.Writer) *stats {
	now := time.Now()
	return &stats{out: out, start: now, lastReport: now, fails: map[string]int64{}}
}

func (s *stats) fail(f failure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails[f.Kind]++
	s.totalFails++
	if s.printed < maxFailureDetails {
		s.printed++
		fmt.Fprintf(s.out, "FAIL #%d kind=%s seq=%d did=%s: %s\n", s.printed, f.Kind, f.Seq, f.DID, f.Reason)
		if s.printed == maxFailureDetails {
			fmt.Fprintf(s.out, "(further failure details suppressed; counts continue)\n")
		}
	}
}

func (s *stats) failures() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalFails
}

func (s *stats) observeLag(eventTime string) {
	t, err := time.Parse(time.RFC3339Nano, eventTime)
	if err != nil {
		return
	}
	d := time.Since(t)
	s.mu.Lock()
	s.lags = append(s.lags, d)
	s.allLags.add(d)
	s.mu.Unlock()
}

func (s *stats) failBreakdownLocked() string {
	if len(s.fails) == 0 {
		return "none"
	}
	kinds := make([]string, 0, len(s.fails))
	for k := range s.fails {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = fmt.Sprintf("%s=%d", k, s.fails[k])
	}
	return strings.Join(parts, ",")
}

// report prints one progress line covering the interval since the last call.
func (s *stats) report() {
	now := time.Now()
	ev := s.events.Load()
	s.mu.Lock()
	lags := s.lags
	s.lags = nil
	elapsed := now.Sub(s.lastReport).Seconds()
	rate := float64(ev-s.lastEvents) / elapsed
	s.lastReport, s.lastEvents = now, ev
	breakdown := s.failBreakdownLocked()
	s.mu.Unlock()

	p50, p99 := percentiles(lags)
	fmt.Fprintf(s.out, "%s %8.1f ev/s total=%d commits_ok=%d/%d syncs_ok=%d/%d failures=[%s] lag p50=%s p99=%s\n",
		now.Format("15:04:05"), rate, ev, s.commitsOK.Load(), s.commits.Load(), s.syncsOK.Load(), s.syncs.Load(),
		breakdown, fmtDur(p50, len(lags)), fmtDur(p99, len(lags)))
}

func (s *stats) summary(lastSeq int64, firstSeq int64, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el := time.Since(s.start)
	ev := s.events.Load()
	fmt.Fprintf(s.out, "\n=== vlpds firehose checker summary (%s) ===\n", reason)
	fmt.Fprintf(s.out, "duration:        %s (%.1f ev/s)\n", el.Round(time.Millisecond), float64(ev)/el.Seconds())
	if firstSeq >= 0 {
		fmt.Fprintf(s.out, "seq range:       %d .. %d\n", firstSeq, lastSeq)
	}
	fmt.Fprintf(s.out, "events:          %d\n", ev)
	fmt.Fprintf(s.out, "  #commit:       %d (%d verified clean)\n", s.commits.Load(), s.commitsOK.Load())
	fmt.Fprintf(s.out, "  #sync:         %d (%d verified clean)\n", s.syncs.Load(), s.syncsOK.Load())
	fmt.Fprintf(s.out, "  #identity:     %d\n", s.identity.Load())
	fmt.Fprintf(s.out, "  #account:      %d\n", s.account.Load())
	fmt.Fprintf(s.out, "  #info:         %d\n", s.info.Load())
	fmt.Fprintf(s.out, "  unknown:       %d\n", s.unknown.Load())
	fmt.Fprintf(s.out, "lag (all):       p50=%s p99=%s max=%s\n",
		fmtDur(s.allLags.quantile(0.5), s.allLags.n), fmtDur(s.allLags.quantile(0.99), s.allLags.n), fmtDur(s.allLags.max, s.allLags.n))
	fmt.Fprintf(s.out, "failures:        %d [%s]\n", s.totalFails, s.failBreakdownLocked())
	if s.totalFails == 0 {
		fmt.Fprintf(s.out, "RESULT: PASS\n")
	} else {
		fmt.Fprintf(s.out, "RESULT: FAIL\n")
	}
}

func percentiles(d []time.Duration) (time.Duration, time.Duration) {
	if len(d) == 0 {
		return 0, 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[(len(d)-1)*50/100], d[(len(d)-1)*99/100]
}

func fmtDur(d time.Duration, n int) string {
	if n == 0 {
		return "-"
	}
	switch {
	case d < time.Millisecond && d > -time.Millisecond:
		return d.Round(time.Microsecond).String()
	case d < time.Second && d > -time.Second:
		return d.Round(100 * time.Microsecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}

// lagHist is a log-bucketed histogram (about 4% resolution) so whole-run
// percentiles don't need every sample kept in memory.
type lagHist struct {
	buckets map[int]int
	n       int
	max     time.Duration
}

const lagHistBase = 1.04

func (h *lagHist) add(d time.Duration) {
	if h.buckets == nil {
		h.buckets = map[int]int{}
	}
	h.n++
	if d > h.max || h.n == 1 {
		h.max = d
	}
	h.buckets[lagBucket(d)]++
}

func lagBucket(d time.Duration) int {
	us := d.Microseconds()
	if us <= 1 {
		return 0
	}
	return int(math.Ceil(math.Log(float64(us)) / math.Log(lagHistBase)))
}

func (h *lagHist) quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	keys := make([]int, 0, len(h.buckets))
	for k := range h.buckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	target := int(q * float64(h.n-1))
	seen := 0
	for _, k := range keys {
		seen += h.buckets[k]
		if seen > target {
			// Bucket upper bound, clamped so p99 never reads above the true max.
			return min(time.Duration(math.Pow(lagHistBase, float64(k)))*time.Microsecond, h.max)
		}
	}
	return h.max
}
