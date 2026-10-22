import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path

from job_queue import claim, connect, enqueue, fail, finish


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "jobs.db")
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_idempotency_and_concurrent_claims(self):
        ids = [enqueue(self.db, {"kind": "sum", "values": [i]}, key=f"k{i}", now=0) for i in range(20)]
        self.assertEqual(ids[0], enqueue(self.db, {"ignored": True}, key="k0", now=0))
        taken = []
        lock = threading.Lock()

        def worker(n):
            with closing(connect(self.path)) as db:
                while job := claim(db, f"worker-{n}", now=0):
                    with lock:
                        taken.append(job["id"])
                    finish(db, job["id"], f"worker-{n}", job["id"])

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertCountEqual(taken, ids)

    def test_retry_backoff_and_dead_letter(self):
        job_id = enqueue(self.db, {"kind": "bad"}, max_attempts=2, now=0)
        job = claim(self.db, "a", now=0)
        self.assertTrue(fail(self.db, job, "a", "first", now=0))
        self.assertIsNone(claim(self.db, "b", now=1))
        job = claim(self.db, "b", now=2)
        self.assertEqual(job["attempts"], 2)
        self.assertTrue(fail(self.db, job, "b", "second", now=2))
        self.assertEqual(self.db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()[0], "dead")

    def test_expired_lease_reclaim(self):
        enqueue(self.db, {"kind": "sum", "values": [1, 2]}, now=0)
        first = claim(self.db, "old", lease_seconds=3, now=0)
        self.assertIsNone(claim(self.db, "new", now=2))
        second = claim(self.db, "new", now=3)
        self.assertEqual(first["id"], second["id"])
        self.assertFalse(finish(self.db, first["id"], "old", 99))
        self.assertTrue(finish(self.db, second["id"], "new", 3))

