# Peer mTLS A/B (2026-10-02, laptop)

`cargo test --profile dev-release --test all peer_tls::bench_ab -- --ignored --nocapture`
on an Apple M4 Pro (other agents' work idle). Two in-process nodes, in-memory
store, split listeners (public + peer) either way; `h2c` = cleartext peer
listener, `mtls` = TLS 1.3 (ring, ECDSA P-256 certs) with client certs. Two
rounds, modes alternating. CPU is the whole process (both nodes and the
load generator). Raw rows: `ab.csv`.

| phase | mode | req/s | CPU µs/req | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| forwarded getRecord (20k, 64 in flight) | h2c | 69.2k / 71.5k | 108.4 / 104.5 | 0.92 / 0.89 | 1.38 / 1.34 |
| | mtls | 71.2k / 72.0k | 105.3 / 104.3 | 0.89 / 0.88 | 1.34 / 1.31 |
| forwarded createRecord (2k, 64 in flight) | h2c | 55.1k / 56.8k | 154.7 / 150.6 | 1.14 / 1.10 | 1.66 / 1.59 |
| | mtls | 56.3k / 55.9k | 153.3 / 154.3 | 1.11 / 1.13 | 1.63 / 1.69 |
| write at owner -> peer's merged firehose (300 sequential; log stream) | h2c | - | 1042 / 1190 | 5.34 / 4.96 | 8.08 / 8.33 |
| | mtls | - | 1048 / 1216 | 4.99 / 5.09 | 6.92 / 7.21 |

No difference beyond run-to-run noise (~3%): handshakes happen once per
pooled connection (`--peer-connections` x 2 per peer, plus one per log
stream), and the AES-GCM record layer on ~1 KB forwards is a few µs against
~105 µs of request CPU. The firehose phase is dominated by the merge
(min-watermark heartbeats), not the stream transport.
