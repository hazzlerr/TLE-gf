"""Tests for the off-site backup staleness watchdog in meta.py.

The watchdog reads kvs['last_backup_at'] (stamped by tle-backup-service) and
pings admins in the logging channel when it goes stale -- but only once at least
one backup has ever been recorded, and only re-pinging every 6h while stale.

These tests drive the decision logic (`_backup_watchdog_task`) directly with a
real in-memory kvs table and a stubbed `_send_backup_alert`, so no Discord
plumbing is needed. Timestamps are expressed relative to real `time.time()` to
avoid patching the clock.
"""
import asyncio
import time

import pytest

from tests.test_kvs import FakeKvsDb
from tle.util import codeforces_common as cf_common
from tle.cogs.meta import (
    Meta,
    _BACKUP_TS_KEY,
    _BACKUP_ALERT_PING_KEY,
    _BACKUP_ALERT_DISABLED_KEY,
    _BACKUP_STALE_THRESHOLD,
    _BACKUP_ALERT_INTERVAL,
)

HOUR = 60 * 60

# The raw coroutine behind the TaskSpec descriptor. The real tasks.TaskSpec
# stores it on `.func`; the test stub in conftest.py stores it on `._func`.
_spec = Meta.__dict__['_backup_watchdog_task']
_watchdog_func = getattr(_spec, 'func', None) or _spec._func


@pytest.fixture
def db(monkeypatch):
    d = FakeKvsDb()
    monkeypatch.setattr(cf_common, 'user_db', d, raising=False)
    yield d
    d.conn.close()


@pytest.fixture
def cog():
    """A Meta cog whose _send_backup_alert is replaced by a recording stub."""
    meta = Meta(bot=object())
    calls = []

    async def fake_send(last_backup, now):
        calls.append((last_backup, now))
        return True  # pretend the alert was delivered

    meta._send_backup_alert = fake_send
    meta.sent = calls
    return meta


def run_watchdog(cog):
    asyncio.run(_watchdog_func(cog, None))


# ---------------------------------------------------------------------------
# The "only if backups were previously done" guard
# ---------------------------------------------------------------------------

class TestNeverBackedUp:
    def test_no_stamp_no_alert(self, db, cog):
        # kvs empty -> backups never ran -> stay silent.
        run_watchdog(cog)
        assert cog.sent == []
        assert db.kvs_get(_BACKUP_ALERT_PING_KEY) is None

    def test_invalid_stamp_no_alert(self, db, cog):
        db.kvs_set(_BACKUP_TS_KEY, 'not-a-number')
        run_watchdog(cog)
        assert cog.sent == []


# ---------------------------------------------------------------------------
# Healthy backups
# ---------------------------------------------------------------------------

class TestHealthy:
    def test_recent_backup_no_alert(self, db, cog):
        db.kvs_set(_BACKUP_TS_KEY, str(time.time() - HOUR))
        run_watchdog(cog)
        assert cog.sent == []

    def test_recovery_clears_ping_state(self, db, cog):
        # A previous outage left a ping timestamp; a fresh backup should clear it
        # so the next outage alerts immediately rather than waiting out 6h.
        db.kvs_set(_BACKUP_TS_KEY, str(time.time() - HOUR))
        db.kvs_set(_BACKUP_ALERT_PING_KEY, str(time.time() - 2 * HOUR))
        run_watchdog(cog)
        assert cog.sent == []
        assert db.kvs_get(_BACKUP_ALERT_PING_KEY) is None


# ---------------------------------------------------------------------------
# Stale backups -> alert
# ---------------------------------------------------------------------------

class TestStale:
    def test_stale_backup_alerts_and_records(self, db, cog):
        last = time.time() - (_BACKUP_STALE_THRESHOLD + HOUR)
        db.kvs_set(_BACKUP_TS_KEY, str(last))
        run_watchdog(cog)
        assert len(cog.sent) == 1
        # Ping timestamp recorded so we don't immediately re-ping.
        assert db.kvs_get(_BACKUP_ALERT_PING_KEY) is not None

    def test_just_under_threshold_no_alert(self, db, cog):
        db.kvs_set(_BACKUP_TS_KEY, str(time.time() - (_BACKUP_STALE_THRESHOLD - HOUR)))
        run_watchdog(cog)
        assert cog.sent == []

    def test_no_reping_within_interval(self, db, cog):
        last = time.time() - (_BACKUP_STALE_THRESHOLD + 2 * HOUR)
        db.kvs_set(_BACKUP_TS_KEY, str(last))
        db.kvs_set(_BACKUP_ALERT_PING_KEY, str(time.time() - HOUR))  # pinged 1h ago
        run_watchdog(cog)
        assert cog.sent == []

    def test_reping_after_interval(self, db, cog):
        last = time.time() - (_BACKUP_STALE_THRESHOLD + 10 * HOUR)
        db.kvs_set(_BACKUP_TS_KEY, str(last))
        db.kvs_set(_BACKUP_ALERT_PING_KEY,
                   str(time.time() - (_BACKUP_ALERT_INTERVAL + HOUR)))
        run_watchdog(cog)
        assert len(cog.sent) == 1

    def test_corrupt_ping_timestamp_alerts(self, db, cog):
        # A garbage ping timestamp should not block alerting.
        db.kvs_set(_BACKUP_TS_KEY, str(time.time() - (_BACKUP_STALE_THRESHOLD + HOUR)))
        db.kvs_set(_BACKUP_ALERT_PING_KEY, 'garbage')
        run_watchdog(cog)
        assert len(cog.sent) == 1


# ---------------------------------------------------------------------------
# Disable switch
# ---------------------------------------------------------------------------

class TestDisabled:
    def test_disabled_suppresses_alert(self, db, cog):
        db.kvs_set(_BACKUP_TS_KEY, str(time.time() - (_BACKUP_STALE_THRESHOLD + HOUR)))
        db.kvs_set(_BACKUP_ALERT_DISABLED_KEY, '1')
        run_watchdog(cog)
        assert cog.sent == []

    def test_send_failure_does_not_record_ping(self, db, cog):
        # If delivery fails, we must NOT stamp the ping key, so the next poll
        # retries instead of going silent for 6h.
        async def failing_send(last_backup, now):
            return False
        cog._send_backup_alert = failing_send
        db.kvs_set(_BACKUP_TS_KEY, str(time.time() - (_BACKUP_STALE_THRESHOLD + HOUR)))
        run_watchdog(cog)
        assert db.kvs_get(_BACKUP_ALERT_PING_KEY) is None
