### Per-hour medians of calm samples (no op in progress or within 40 s of one; 5 min warmup after each start)

| soak h | phase(s) | n | write p50 ms | write p99 ms | getRecord p99 ms | listRecords p99 ms | listRecords delete-heavy p99 ms | obj-store req/commit | state SST GET+HEAD / read | assign/ GETs/s | firehose lag p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0-1 | restart | 183 | 22.49 | 58.97 | 1.81 | 2.18 | 2.65 | 1.0 | 3.8 | 0.0 | 73.64 |
| 1-2 | calm,restart | 280 | 22.99 | 52.04 | 2.01 | 2.71 | 2.92 | 1.22 | 5.22 | 0.0 | 65.22 |
| 2-3 | reshard | 197 | 23.25 | 69.99 | 2.36 | 3.09 | 3.37 | 1.98 | 9.61 | 0.0 | 79.37 |
| 3-4 | calm,reshard | 278 | 23.41 | 75.36 | 2.46 | 3.25 | 3.79 | 1.88 | 9.36 | 0.0 | 85.47 |
| 4-5 | mixed | 152 | 23.44 | 60.72 | 2.04 | 2.33 | 3.09 | 1.75 | 8.38 | 0.0 | 75.35 |
| 5-6 | restart | 196 | 23.88 | 68.48 | 1.99 | 2.16 | 3.06 | 1.85 | 9.52 | 0.0 | 83.53 |
| 6-7 | calm,restart | 278 | 24.45 | 79.26 | 2.07 | 2.52 | 3.18 | 1.37 | 6.35 | 0.0 | 96.32 |

### Stall windows (10 s samples with write p50 > 200 ms or p99 > 1 s), per soak hour and phase

| soak h | samples | stalls in restart | in reshard | in calm | in mixed | of which within 30 s of an op |
|---|---|---|---|---|---|---|
| 0-1 | 359 | 30 | 0 | 0 | 0 | 30 |
| 1-2 | 360 | 10 | 0 | 4 | 0 | 10 |
| 2-3 | 360 | 0 | 9 | 0 | 0 | 8 |
| 3-4 | 360 | 0 | 8 | 6 | 0 | 8 |
| 4-5 | 360 | 0 | 0 | 0 | 37 | 37 |
| 5-6 | 360 | 29 | 0 | 0 | 0 | 29 |
| 6-7 | 359 | 14 | 0 | 8 | 0 | 14 |

### Restart cost by restart ordinal (bins of ~20)

| restarts # | soak h | SIGTERM exit s (med) | serving s (med) | converged s (med) | kill -9 share | max write p99 in 40 s after (med, SIGTERM) | req/commit in 40 s after (med) |
|---|---|---|---|---|---|---|---|
| 1-20 | 0.00-0.48 | 0.9 | 0.24 | 5.08 | 5/20 | 309.91 | 2.02 |
| 21-40 | 0.50-0.98 | 1.0 | 0.3 | 5.08 | 5/20 | 265.07 | 2.34 |
| 41-60 | 1.00-1.48 | 1.25 | 0.27 | 5.34 | 2/20 | 403.41 | 2.73 |
| 61-80 | 4.00-4.48 | 1.62 | 0.29 | 4.87 | 6/20 | 677.25 | 3.63 |
| 81-100 | 4.50-4.98 | 1.61 | 0.25 | 5.1 | 5/20 | 694.53 | 3.75 |
| 101-120 | 5.00-5.48 | 1.7 | 0.27 | 5.1 | 6/20 | 725.25 | 3.65 |
| 121-140 | 5.51-5.98 | 1.7 | 0.26 | 5.62 | 3/20 | 884.96 | 3.87 |
| 141-160 | 6.01-6.48 | 1.75 | 0.25 | 4.86 | 6/20 | 774.81 | 4.11 |

### Reshard ops by ordinal (bins of 10)

| reshards # | soak h | op secs (med / max) | converge s (med) | retired dirs after | retired GB after | pinned GB after | assign/ objects after | assign/ LIST B after |
|---|---|---|---|---|---|---|---|---|
| 1-10 | 2.00-2.23 | 0.39 / 1.0 | 0.27 | 15 | 0.54 | 0.54 | 80 | 17038 |
| 11-20 | 2.25-2.48 | 0.4 / 0.68 | 0.01 | 30 | 1.15 | 1.01 | 95 | 20173 |
| 21-30 | 2.50-2.73 | 0.41 / 0.82 | 0.51 | 45 | 1.59 | 1.21 | 110 | 23309 |
| 31-40 | 2.75-2.98 | 0.43 / 0.89 | 0.01 | 60 | 2.25 | 1.49 | 125 | 26444 |
| 41-50 | 3.00-3.23 | 0.48 / 0.83 | 0.26 | 75 | 2.82 | 1.62 | 140 | 29579 |
| 51-60 | 3.25-3.48 | 0.43 / 0.76 | 0.51 | 90 | 3.37 | 1.86 | 155 | 32714 |
| 61-70 | 4.00-4.23 | 0.59 / 3.46 | 0.52 | 105 | 4.34 | 1.88 | 170 | 35849 |
| 71-80 | 4.25-4.48 | 0.42 / 1.33 | 0.52 | 120 | 4.91 | 2.12 | 185 | 38986 |
| 81-90 | 4.50-4.73 | 0.43 / 2.18 | 0.53 | 135 | 5.62 | 2.41 | 200 | 42149 |
| 91-100 | 4.76-4.98 | 0.47 / 0.93 | 0.53 | 150 | 6.44 | 2.73 | 215 | 45298 |

### n1 memory (n1 is restarted only by the window-2 resume at 4.83 h)

| soak h | RSS MB | jemalloc allocated MB | repo cache MB | in-memory caches MB | tokio tasks | /metrics series |
|---|---|---|---|---|---|---|
| 0.00 | 1200.1 | 749.6 | 165.7 | 1.67 | 841.0 | 1486 |
| 0.25 | 3069.3 | 2324.1 | 126.7 | 28.2 | 655.0 | 1523 |
| 0.75 | 4423.4 | 3485.8 | 322.5 | 59.27 | 615.0 | 1525 |
| 1.25 | 4569.6 | 3550.3 | 409.4 | 26.61 | 611.0 | 1548 |
| 1.75 | 4894.5 | 3915.8 | 705.2 | 52.39 | 607.0 | 1548 |
| 2.25 | 5044.0 | 4044.1 | 814.6 | 67.04 | 611.0 | 1620 |
| 2.75 | 5413.5 | 4428.7 | 1137.6 | 79.32 | 613.0 | 1623 |
| 3.25 | 5480.8 | 4299.0 | 970.0 | 75.79 | 629.0 | 1623 |
| 3.75 | 5588.3 | 4595.1 | 1265.8 | 68.01 | 647.0 | 1623 |
| 4.25 | 5956.4 | 4481.0 | 1027.3 | 76.87 | 639.0 | 1623 |
| 4.75 | 6293.7 | 4493.9 | 970.7 | 15.89 | 612.0 | 1630 |
| 5.25 | 5472.9 | 4269.5 | 1070.4 | 51.39 | 605.0 | 1598 |
| 5.75 | 5915.9 | 4601.6 | 1295.5 | 60.87 | 607.0 | 1601 |
| 6.25 | 6179.0 | 5017.9 | 1716.9 | 32.8 | 622.0 | 1601 |
| 6.75 | 6324.1 | 5092.2 | 1710.0 | 58.71 | 605.0 | 1601 |

### Client errors after retries, by phase

| phase | op | errors |
|---|---|---|
| mixed | r_syncGetRecord | 1 |
| mixed | w_create | 103 |
| mixed | w_delete | 5 |

First error texts: `w_create`: 500 b'{"error":"InternalServerError","message":"repo load failed: Closed error: db is closed"}'; `w_delete`: 500 b'{"error":"InternalServerError","message":"repo load failed: Closed error: db is closed"}'; `r_syncGetRecord`: 500 b'{"error":"InternalServerError","message":"repo load failed: Closed error: db is closed"}'
