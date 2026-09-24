# PUT latency

- cluster: 3 nodes on 127.0.0.1, one client, sequential PUTs
- measured PUTs per scenario: 500 (plus warmup)
- generated: 2026-09-24T13:39:18-07:00

| scenario | mean (ms) | p50 (ms) | p99 (ms) | max (ms) |
| --- | --- | --- | --- | --- |
| 3 nodes healthy | 24.23 | 23.16 | 54.08 | 114.79 |
| 1 follower killed | 20.78 | 18.75 | 42.07 | 524.11 |
