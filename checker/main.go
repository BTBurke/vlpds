// Command checker is an independent sync 1.1 conformance oracle for an atproto
// firehose (com.atproto.sync.subscribeRepos), written to test the vlpds PDS.
//
// For every event it checks: strictly increasing seq (dense with -dense); #commit CAR
// integrity (block hashes, root == commit CID, DID/rev match), the sync 1.1 MST
// inversion against prevData (indigo repo.VerifyCommitMessage), the commit
// signature against the key from the PDS's describeRepo, and per-DID chain
// continuity (since == previous rev, prevData == previous data CID, rev
// strictly increasing). #sync events are signature-checked and reset the chain.
// Every frame header/body and every DAG-CBOR block must be canonical DAG-CBOR;
// op paths are unique within a commit; blocks fit the lexicon's maxLength;
// #identity/#account fields are well-formed; and per-DID event times never go
// backwards.
//
// Usage:
//
//	go run . -host http://localhost:2583                  # tail from now, forever
//	go run . -host http://localhost:2583 -cursor 0 -max-events 10000 -strict
//	go build -o checker . && ./checker -quiet -strict -cursor 0
//
// Flags: -host, -cursor (replay from seq; omit to start live), -max-events (0 =
// unlimited), -workers (default NumCPU), -strict (exit 1 on any failure),
// -quiet (no 5 s progress lines). Exit codes: 0 ok, 1 failures under -strict,
// 2 could not connect / bad flags.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	os.Exit(run())
}

type config struct {
	host      string
	cursor    int64
	hasCursor bool
	maxEvents int64
	workers   int
	strict    bool
	quiet     bool
	dense     bool
}

func parseFlags() (config, error) {
	var c config
	var cursor string
	flag.StringVar(&c.host, "host", "http://localhost:2583", "PDS base URL (http/https or ws/wss)")
	flag.StringVar(&cursor, "cursor", "", "optional seq cursor to replay from")
	flag.Int64Var(&c.maxEvents, "max-events", 0, "stop after N events (0 = unlimited)")
	flag.IntVar(&c.workers, "workers", runtime.NumCPU(), "verification goroutines (partitioned by DID)")
	flag.BoolVar(&c.strict, "strict", false, "exit non-zero if any failure was seen")
	flag.BoolVar(&c.quiet, "quiet", false, "suppress the periodic progress line")
	flag.BoolVar(&c.dense, "dense", false, "require dense seqs (each seq == previous+1)")
	flag.Parse()
	if cursor != "" {
		n, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return c, fmt.Errorf("-cursor: %w", err)
		}
		c.cursor, c.hasCursor = n, true
	}
	if c.workers < 1 {
		c.workers = 1
	}
	return c, nil
}

func subscribeURL(host string, cursor int64, hasCursor bool) (string, error) {
	u, err := url.Parse(strings.TrimRight(host, "/"))
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "ws":
		u.Scheme = "ws"
	case "https", "wss":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme %q in -host", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/xrpc/com.atproto.sync.subscribeRepos"
	if hasCursor {
		u.RawQuery = "cursor=" + strconv.FormatInt(cursor, 10)
	}
	return u.String(), nil
}

// httpHost returns the http(s) form of -host for XRPC GETs.
func httpHost(host string) string {
	switch {
	case strings.HasPrefix(host, "ws://"):
		return "http://" + strings.TrimPrefix(host, "ws://")
	case strings.HasPrefix(host, "wss://"):
		return "https://" + strings.TrimPrefix(host, "wss://")
	}
	return host
}

func run() int {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// indigo's VerifyCommitMessage logs at Info/Warn for every event; we report
	// those conditions ourselves.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	wsURL, err := subscribeURL(cfg.host, cfg.cursor, cfg.hasCursor)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("connecting to %s (workers=%d)\n", wsURL, cfg.workers)
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			fmt.Fprintf(os.Stderr, "dial %s: %v (HTTP %d: %s)\n", wsURL, err, resp.StatusCode, body)
		} else {
			fmt.Fprintf(os.Stderr, "dial %s: %v\n", wsURL, err)
		}
		return 2
	}
	conn.SetReadLimit(64 << 20)
	go func() {
		<-ctx.Done()
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		conn.Close()
	}()

	st := newStats(os.Stdout)
	keys := newPDSKeySource(httpHost(cfg.host))
	pool := startWorkers(cfg.workers, keys, st)

	reportDone := make(chan struct{})
	reportStopped := make(chan struct{})
	go func() {
		defer close(reportStopped)
		if cfg.quiet {
			<-reportDone
			return
		}
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				st.report()
			case <-reportDone:
				return
			}
		}
	}()

	sc := newSeqChecker()
	sc.allowGaps = !cfg.dense
	reason := readLoop(ctx, conn, cfg, st, sc, pool)

	pool.close()
	close(reportDone)
	<-reportStopped
	st.summary(sc.last, sc.first, reason)

	if cfg.strict && st.failures() > 0 {
		return 1
	}
	return 0
}

// readLoop reads frames until the connection ends, max-events is hit, or ctx
// is cancelled, and returns why it stopped.
func readLoop(ctx context.Context, conn *websocket.Conn, cfg config, st *stats, sc *seqChecker, pool *workerPool) string {
	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return "interrupted"
			}
			var ce *websocket.CloseError
			if errors.As(err, &ce) {
				return fmt.Sprintf("connection closed by server: %d %s", ce.Code, ce.Text)
			}
			return fmt.Sprintf("connection error: %v", err)
		}
		if mt != websocket.BinaryMessage {
			st.fail(failure{Kind: failDecode, Seq: sc.last, Reason: fmt.Sprintf("non-binary websocket message type %d", mt)})
			continue
		}
		ev, err := decodeFrame(msg)
		if err != nil {
			st.fail(failure{Kind: failDecode, Seq: sc.last, Reason: err.Error()})
			continue
		}
		st.events.Add(1)
		if ev.NonCanonical != "" {
			seq, ok := ev.seq()
			if !ok {
				seq = sc.last
			}
			st.fail(failure{Kind: failNonCanonical, Seq: seq, DID: ev.did(), Reason: ev.NonCanonical})
		}

		if ev.Header.Op == frameOpError {
			st.fail(failure{Kind: failUpstreamError, Seq: sc.last, Reason: ev.ErrName + ": " + ev.ErrMsg})
			return "server error frame: " + ev.ErrName
		}
		if ev.Info != nil {
			st.info.Add(1)
			msg := ""
			if ev.Info.Message != nil {
				msg = *ev.Info.Message
			}
			fmt.Printf("INFO frame: %s %s\n", ev.Info.Name, msg)
		}
		if seq, ok := ev.seq(); ok {
			if f := sc.check(seq); f != nil {
				f.DID = ev.did()
				st.fail(*f)
			}
			st.observeLag(ev.time())
		}
		switch {
		case ev.Commit != nil, ev.Sync != nil, ev.Identity != nil:
			pool.dispatch(ev)
		case ev.Account != nil:
			st.account.Add(1)
			pool.dispatch(ev)
		case ev.Info == nil:
			st.unknown.Add(1)
		}

		if cfg.maxEvents > 0 && st.events.Load() >= cfg.maxEvents {
			return fmt.Sprintf("max-events %d reached", cfg.maxEvents)
		}
	}
}

// seqChecker enforces that seq is strictly increasing, and dense when
// requested. vlpds seqs are time-based (unique, increasing, with gaps).
type seqChecker struct {
	first, last int64
	allowGaps   bool
}

func newSeqChecker() *seqChecker { return &seqChecker{first: -1, last: -1} }

func (s *seqChecker) check(seq int64) *failure {
	if s.first < 0 {
		s.first, s.last = seq, seq
		return nil
	}
	prev := s.last
	switch {
	case seq <= prev:
		// Don't move last backwards: keep measuring density against the high-water mark.
		return &failure{Kind: failSeqReorder, Seq: seq, Reason: fmt.Sprintf("seq %d after %d", seq, prev)}
	case seq != prev+1 && !s.allowGaps:
		s.last = seq
		return &failure{Kind: failSeqGap, Seq: seq, Reason: fmt.Sprintf("seq jumped %d -> %d (%d missing)", prev, seq, seq-prev-1)}
	}
	s.last = seq
	return nil
}

type workerPool struct {
	chans []chan *event
	wg    sync.WaitGroup
}

func startWorkers(n int, keys KeySource, st *stats) *workerPool {
	p := &workerPool{chans: make([]chan *event, n)}
	for i := range p.chans {
		ch := make(chan *event, 1024)
		p.chans[i] = ch
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			// Verification uses its own context so events already queued at
			// SIGINT are still checked properly before the summary.
			ctx := context.Background()
			v := newVerifier(keys)
			for ev := range ch {
				handleEvent(ctx, v, keys, st, ev)
			}
		}()
	}
	return p
}

func (p *workerPool) dispatch(ev *event) {
	h := fnv.New32a()
	h.Write([]byte(ev.did()))
	p.chans[h.Sum32()%uint32(len(p.chans))] <- ev
}

func (p *workerPool) close() {
	for _, ch := range p.chans {
		close(ch)
	}
	p.wg.Wait()
}

func handleEvent(ctx context.Context, v *verifier, keys KeySource, st *stats, ev *event) {
	switch {
	case ev.Commit != nil:
		st.commits.Add(1)
		fails := v.verifyCommit(ctx, ev.Commit)
		for _, f := range fails {
			st.fail(f)
		}
		if len(fails) == 0 {
			st.commitsOK.Add(1)
		}
	case ev.Sync != nil:
		st.syncs.Add(1)
		fails := v.verifySync(ctx, ev.Sync)
		for _, f := range fails {
			st.fail(f)
		}
		if len(fails) == 0 {
			st.syncsOK.Add(1)
		}
	case ev.Identity != nil:
		st.identity.Add(1)
		for _, f := range v.verifyIdentity(ev.Identity) {
			st.fail(f)
		}
		// Handled in the DID's worker so the key refresh is ordered with its commits.
		if keys != nil {
			keys.Invalidate(ev.Identity.Did)
		}
	case ev.Account != nil:
		// counted in readLoop
		for _, f := range v.verifyAccount(ev.Account) {
			st.fail(f)
		}
	}
}
