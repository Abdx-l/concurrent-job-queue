"""SQLite-backed queue with leases, backoff, and idempotent submission."""
import argparse
import json
import sqlite3
import time
import uuid
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
 id INTEGER PRIMARY KEY,
 payload TEXT NOT NULL,
 idempotency_key TEXT UNIQUE,
 status TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0,
 max_attempts INTEGER NOT NULL DEFAULT 3,
 available_at REAL NOT NULL,
 lease_until REAL,
 owner TEXT,
 result TEXT,
 error TEXT,
 created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status, available_at, lease_until);
"""


def connect(path):
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
    return db


def enqueue(db, payload, key=None, max_attempts=3, now=None):
    now = time.time() if now is None else now
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    encoded = json.dumps(payload, separators=(",", ":"))
    db.execute("BEGIN IMMEDIATE")
    try:
        if key is not None:
            found = db.execute("SELECT id FROM jobs WHERE idempotency_key=?", (key,)).fetchone()
            if found:
                db.commit()
                return found["id"]
        cursor = db.execute(
            "INSERT INTO jobs(payload,idempotency_key,max_attempts,available_at,created_at) VALUES(?,?,?,?,?)",
            (encoded, key, max_attempts, now, now),
        )
        db.commit()
        return cursor.lastrowid
    except BaseException:
        db.rollback()
        raise


def claim(db, owner, lease_seconds=30, now=None):
    now = time.time() if now is None else now
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            """SELECT * FROM jobs WHERE
            (status='pending' AND available_at<=?) OR
            (status='running' AND lease_until<=?)
            ORDER BY available_at,id LIMIT 1""", (now, now)
        ).fetchone()
        if row is None:
            db.commit()
            return None
        if row["attempts"] >= row["max_attempts"]:
            db.execute("UPDATE jobs SET status='dead',owner=NULL,lease_until=NULL,error='Lease expired after final attempt' WHERE id=?", (row["id"],))
            db.commit()
            return claim(db, owner, lease_seconds, now)
        db.execute(
            "UPDATE jobs SET status='running',owner=?,lease_until=?,attempts=attempts+1 WHERE id=?",
            (owner, now + lease_seconds, row["id"]),
        )
        claimed = db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
        db.commit()
        return dict(claimed)
    except BaseException:
        db.rollback()
        raise


def finish(db, job_id, owner, result):
    cursor = db.execute(
        "UPDATE jobs SET status='done',result=?,owner=NULL,lease_until=NULL WHERE id=? AND owner=? AND status='running'",
        (json.dumps(result), job_id, owner),
    )
    return cursor.rowcount == 1


def fail(db, job, owner, error, now=None, base_delay=2):
    now = time.time() if now is None else now
    exhausted = job["attempts"] >= job["max_attempts"]
    status = "dead" if exhausted else "pending"
    delay = 0 if exhausted else base_delay * 2 ** (job["attempts"] - 1)
    cursor = db.execute(
        """UPDATE jobs SET status=?,available_at=?,error=?,owner=NULL,lease_until=NULL
        WHERE id=? AND owner=? AND status='running'""",
        (status, now + delay, str(error)[:1000], job["id"], owner),
    )
    return cursor.rowcount == 1


def handle(payload):
    kind = payload.get("kind")
    if kind == "sum":
        return sum(payload["values"])
    if kind == "sleep":
        seconds = float(payload["seconds"])
        if not 0 <= seconds <= 60:
            raise ValueError("sleep seconds must be between 0 and 60")
        time.sleep(seconds)
        return "ok"
    raise ValueError(f"Unknown job kind: {kind}")


def work_once(db, owner=None, handler=handle, now=None):
    owner = owner or str(uuid.uuid4())
    job = claim(db, owner, now=now)
    if job is None:
        return None
    try:
        result = handler(json.loads(job["payload"]))
    except Exception as exc:
        fail(db, job, owner, exc, now=now)
        return {"id": job["id"], "error": str(exc)}
    finish(db, job["id"], owner, result)
    return {"id": job["id"], "result": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="jobs.db")
    sub = parser.add_subparsers(dest="command", required=True)
    add = sub.add_parser("enqueue")
    add.add_argument("payload", help="JSON job payload")
    add.add_argument("--key")
    add.add_argument("--max-attempts", type=int, default=3)
    worker = sub.add_parser("work")
    worker.add_argument("--once", action="store_true")
    sub.add_parser("list")
    args = parser.parse_args()
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    with connect(args.db) as db:
        if args.command == "enqueue":
            print(enqueue(db, json.loads(args.payload), args.key, args.max_attempts))
        elif args.command == "list":
            print(json.dumps([dict(row) for row in db.execute("SELECT * FROM jobs ORDER BY id")], indent=2))
        else:
            owner = str(uuid.uuid4())
            while True:
                print(json.dumps(work_once(db, owner)))
                if args.once:
                    break
                time.sleep(1)


if __name__ == "__main__":
    main()

