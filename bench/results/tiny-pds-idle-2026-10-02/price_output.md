| run | Class A /s | Class B /s | Class A /mo | Class B /mo | R2 $/mo (bulk deletes free) | S3 $/mo |
|---|---|---|---|---|---|---|
| idle1 | 0.296 | 0.556 | 0.78 M | 1.46 M | $0.00 ($0.00) | $4.43 |
| pers1 | 0.586 | 1.498 | 1.54 M | 3.94 M | $2.42 ($1.47) | $8.20 |
| idle64 | 2.320 | 28.520 | 6.10 M | 74.95 M | $46.32 ($44.22) | $58.13 |
| tidle1 | 0.096 | 0.226 | 0.25 M | 0.59 M | $0.00 ($0.00) | $1.46 |
| tpers1 | 0.401 | 2.798 | 1.05 M | 7.35 M | $0.24 ($0.00) | $7.12 |
| bidle1 | 0.296 | 0.384 | 0.78 M | 1.01 M | $0.00 ($0.00) | $4.25 |
| bpers1 | 0.584 | 1.453 | 1.53 M | 3.82 M | $2.41 ($1.45) | $8.13 |

**pers1 vs idle1**: 68 commits ({'like': 30, 'getRepo': 6, 'getBlob': 15, 'follow': 7, 'repost': 8, 'post': 23, 'blob_upload': 8}) in 1800 s; marginal per commit 7.56 Class A (of which 2.07 bulk deletes) + 24.60 Class B; per blob upload 1.00 A; getRepo 0.00 A + 0.00 B; getBlob 1.00 B

| idle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 0.01 GB stored | 0.823 M A | 1.612 M B | R2 $0.00 (bulk deletes free $0.00) | S3 $4.66 |
| idle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 2 GB stored | 0.823 M A | 1.612 M B | R2 $0.00 (bulk deletes free $0.00) | S3 $4.71 |
| idle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 20 GB stored | 0.823 M A | 1.612 M B | R2 $0.15 (bulk deletes free $0.15) | S3 $5.12 |

**tpers1 vs tidle1**: 68 commits ({'like': 33, 'getRepo': 6, 'getBlob': 15, 'follow': 6, 'repost': 9, 'post': 20, 'blob_upload': 8}) in 1800 s; marginal per commit 7.96 Class A (of which 2.10 bulk deletes) + 67.75 Class B; per blob upload 1.00 A; getRepo 0.00 A + 0.00 B; getBlob 1.00 B

| tidle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 0.01 GB stored | 0.300 M A | 1.007 M B | R2 $0.00 (bulk deletes free $0.00) | S3 $1.80 |
| tidle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 2 GB stored | 0.300 M A | 1.007 M B | R2 $0.00 (bulk deletes free $0.00) | S3 $1.85 |
| tidle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 20 GB stored | 0.300 M A | 1.007 M B | R2 $0.15 (bulk deletes free $0.15) | S3 $2.26 |

**bpers1 vs bidle1**: 68 commits ({'like': 44, 'getRepo': 6, 'getBlob': 15, 'follow': 3, 'repost': 4, 'post': 17, 'blob_upload': 8}) in 1800 s; marginal per commit 7.51 Class A (of which 2.07 bulk deletes) + 27.96 Class B; per blob upload 1.00 A; getRepo 0.00 A + 0.00 B; getBlob 1.00 B

| bidle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 0.01 GB stored | 0.823 M A | 1.181 M B | R2 $0.00 (bulk deletes free $0.00) | S3 $4.49 |
| bidle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 2 GB stored | 0.823 M A | 1.181 M B | R2 $0.00 (bulk deletes free $0.00) | S3 $4.53 |
| bidle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 20 GB stored | 0.823 M A | 1.181 M B | R2 $0.15 (bulk deletes free $0.15) | S3 $4.95 |

