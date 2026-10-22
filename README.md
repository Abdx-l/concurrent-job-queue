# Concurrent Job Queue

A durable SQLite queue for small background workloads. Workers claim jobs in transactions, hold time-limited leases, retry failures with exponential backoff, and move exhausted jobs to a dead-letter state. A stable idempotency key prevents duplicate submissions.

```sh
python job_queue.py enqueue '{"kind":"sum","values":[2,3,5]}' --key invoice-42
python job_queue.py work --once
python job_queue.py list
python -m unittest discover -s tests -v
```

The built-in `sum` and `sleep` handlers demonstrate the worker contract. Add handlers to `HANDLERS` for application work. Use `--db` to point multiple worker processes at the same SQLite database. Expired leases become claimable again, so handlers should be safe to run more than once. The queue uses SQLite's single-writer transactions and suits local or modest workloads.

