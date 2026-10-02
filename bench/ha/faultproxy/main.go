// faultproxy: a small controllable proxy for vlpds HA fault injection.
//
// Two modes:
//
//	-mode http  reverse proxy (S3 / MinIO). Faults are per request:
//	            latency, S3-style 5xx/throttling errors, blackhole (requests hang).
//	-mode tcp   byte pipe (a node's peer listener). Faults: latency per
//	            chunk, blackhole (new and existing connections stall).
//
// Control API on -ctl (plain HTTP, idempotent):
//
//	GET /set?blackhole=1&latency_ms=200&jitter_ms=50&err_pct=20&err_code=503
//	GET /clear            remove all faults
//	GET /stats            JSON counters
//
// The Host header is passed through untouched, so S3 SigV4 signatures (which
// cover Host) still verify at MinIO when clients sign for the proxy address.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type faults struct {
	mu        sync.RWMutex
	blackhole bool
	latency   time.Duration
	jitter    time.Duration
	errPct    int
	errCode   int
	// closed and replaced whenever faults are cleared, waking blackholed requests
	release chan struct{}
}

func (f *faults) snapshot() (bool, time.Duration, time.Duration, int, int, chan struct{}) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.blackhole, f.latency, f.jitter, f.errPct, f.errCode, f.release
}

func (f *faults) delay() time.Duration {
	_, lat, jit, _, _, _ := f.snapshot()
	if jit > 0 {
		lat += time.Duration(rand.Int63n(int64(jit)))
	}
	return lat
}

var reqs, errs, holds, conns atomic.Int64

func main() {
	listen := flag.String("listen", "127.0.0.1:9300", "proxy listen address")
	target := flag.String("target", "127.0.0.1:9200", "upstream host:port")
	mode := flag.String("mode", "http", "http | tcp")
	ctl := flag.String("ctl", "127.0.0.1:9301", "control API listen address")
	flag.Parse()

	f := &faults{release: make(chan struct{}), errCode: 503}

	ctlMux := http.NewServeMux()
	ctlMux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		f.mu.Lock()
		if v := q.Get("blackhole"); v != "" {
			f.blackhole = v == "1" || v == "true"
		}
		if v := q.Get("latency_ms"); v != "" {
			n, _ := strconv.Atoi(v)
			f.latency = time.Duration(n) * time.Millisecond
		}
		if v := q.Get("jitter_ms"); v != "" {
			n, _ := strconv.Atoi(v)
			f.jitter = time.Duration(n) * time.Millisecond
		}
		if v := q.Get("err_pct"); v != "" {
			f.errPct, _ = strconv.Atoi(v)
		}
		if v := q.Get("err_code"); v != "" {
			f.errCode, _ = strconv.Atoi(v)
		}
		if !f.blackhole {
			close(f.release)
			f.release = make(chan struct{})
		}
		f.mu.Unlock()
		fmt.Fprintln(w, "ok")
	})
	ctlMux.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.blackhole, f.latency, f.jitter, f.errPct = false, 0, 0, 0
		close(f.release)
		f.release = make(chan struct{})
		f.mu.Unlock()
		fmt.Fprintln(w, "ok")
	})
	ctlMux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		bh, lat, jit, ep, ec, _ := f.snapshot()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"requests": reqs.Load(), "injected_errors": errs.Load(), "held": holds.Load(),
			"conns":     conns.Load(),
			"blackhole": bh, "latency_ms": lat.Milliseconds(), "jitter_ms": jit.Milliseconds(),
			"err_pct": ep, "err_code": ec,
		})
	})
	go func() { log.Fatal(http.ListenAndServe(*ctl, ctlMux)) }()

	switch *mode {
	case "http":
		serveHTTP(*listen, *target, f)
	case "tcp":
		serveTCP(*listen, *target, f)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

// hold blocks while the blackhole is on (or until ctx ends).
func hold(ctx context.Context, f *faults) bool {
	for {
		bh, _, _, _, _, rel := f.snapshot()
		if !bh {
			return true
		}
		holds.Add(1)
		select {
		case <-rel:
		case <-ctx.Done():
			return false
		}
	}
}

func serveHTTP(listen, target string, f *faults) {
	up, _ := url.Parse("http://" + target)
	rp := httputil.NewSingleHostReverseProxy(up)
	orig := rp.Director
	rp.Director = func(r *http.Request) {
		host := r.Host
		orig(r)
		r.Host = host // keep the signed Host header
	}
	rp.Transport = &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
	}
	rp.FlushInterval = -1
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		if !hold(r.Context(), f) {
			return
		}
		if d := f.delay(); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		_, _, _, pct, code, _ := f.snapshot()
		if pct > 0 && rand.Intn(100) < pct {
			errs.Add(1)
			// drain the body so the client sees a clean response
			_, _ = io.Copy(io.Discard, r.Body)
			s3code := "SlowDown"
			if code != 503 {
				s3code = "InternalError"
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(code)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected by faultproxy</Message></Error>`, s3code)
			return
		}
		rp.ServeHTTP(w, r)
	})
	log.Printf("faultproxy http %s -> %s", listen, target)
	log.Fatal(http.ListenAndServe(listen, h))
}

func serveTCP(listen, target string, f *faults) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("faultproxy tcp %s -> %s", listen, target)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		conns.Add(1)
		go func() {
			defer c.Close()
			if !hold(context.Background(), f) {
				return
			}
			u, err := net.DialTimeout("tcp", target, 5*time.Second)
			if err != nil {
				return
			}
			defer u.Close()
			done := make(chan struct{}, 2)
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 64<<10)
				for {
					n, err := src.Read(buf)
					if n > 0 {
						// stall (not drop) while blackholed: like a partition, the
						// peer sees silence, and TCP state survives a heal
						hold(context.Background(), f)
						if d := f.delay(); d > 0 {
							time.Sleep(d)
						}
						if _, werr := dst.Write(buf[:n]); werr != nil {
							break
						}
					}
					if err != nil {
						break
					}
				}
				done <- struct{}{}
			}
			go pipe(u, c)
			go pipe(c, u)
			<-done
		}()
	}
}
