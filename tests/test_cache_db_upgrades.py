"""Tests for cache.db upgrade system and CacheDbConn._run_upgrades."""
import sqlite3
import time
from collections import namedtuple

import pytest

from tle.util.db.cache_db_conn import CacheDbConn
from tle.util.db.cache_db_upgrades import registry


def _make_db():
    """Create a CacheDbConn with in-memory DB via create_tables (no upgrades yet)."""
    db = CacheDbConn.__new__(CacheDbConn)
    db.db_file = ':memory:'
    db.conn = sqlite3.connect(':memory:')
    db.create_tables()
    return db


class TestCacheUpgrade100:
    """Upgrade 1.0.0 creates the handle_alias table (if not already from create_tables)."""

    def test_handle_alias_table_usable_after_upgrade(self):
        db = _make_db()
        db._run_upgrades()

        now = int(time.time())
        db.conn.execute(
            'INSERT INTO handle_alias (handle, current_handle, resolved_at) VALUES (?, ?, ?)',
            ('OldName', 'NewName', now)
        )
        db.conn.commit()

        row = db.conn.execute(
            'SELECT current_handle, resolved_at FROM handle_alias WHERE handle = ?',
            ('OldName',)
        ).fetchone()
        assert row == ('NewName', now)


class TestCacheUpgrade110:
    """Upgrade 1.1.0 clears stale handle_alias entries."""

    def test_clears_stale_aliases(self):
        db = _make_db()
        # Simulate pre-upgrade state: handle_alias has stale data, no version stamped
        db.conn.execute(
            'INSERT INTO handle_alias VALUES (?, ?, ?)',
            ('Friedrich', 'Friedrich', int(time.time()))
        )
        db.conn.execute(
            'INSERT INTO handle_alias VALUES (?, ?, ?)',
            ('LMeyling', 'LMeyling', int(time.time()))
        )
        db.conn.commit()
        assert db.conn.execute('SELECT COUNT(*) FROM handle_alias').fetchone()[0] == 2

        # Run upgrades — detects pre-existing handle_alias, stamps 1.0.0, runs 1.1.0
        db._run_upgrades()

        assert db.conn.execute('SELECT COUNT(*) FROM handle_alias').fetchone()[0] == 0

    def test_version_at_latest_after_upgrade(self):
        db = _make_db()
        # Pre-existing alias data (simulates buggy deployment)
        db.conn.execute(
            'INSERT INTO handle_alias VALUES (?, ?, ?)',
            ('stale', 'stale', 1000)
        )
        db.conn.commit()

        db._run_upgrades()

        from tle.util.db.cache_db_conn import _namedtuple_factory
        db.conn.row_factory = _namedtuple_factory
        version = db.conn.execute(
            'SELECT version FROM cache_db_version LIMIT 1'
        ).fetchone()
        db.conn.row_factory = None
        assert version.version == registry.latest_version


class TestRunUpgradesFreshDb:
    """_run_upgrades on a brand-new DB (no pre-existing data)."""

    def test_fresh_db_stamps_latest_version(self):
        db = _make_db()
        db._run_upgrades()

        from tle.util.db.cache_db_conn import _namedtuple_factory
        db.conn.row_factory = _namedtuple_factory
        version = db.conn.execute(
            'SELECT version FROM cache_db_version LIMIT 1'
        ).fetchone()
        db.conn.row_factory = None
        assert version.version == registry.latest_version

    def test_fresh_db_has_handle_alias_table(self):
        """create_tables creates handle_alias; upgrades stamp and may clear it."""
        db = _make_db()
        db._run_upgrades()

        tables = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='handle_alias'"
        ).fetchone()
        assert tables is not None

    def test_fresh_db_idempotent(self):
        db = _make_db()
        db._run_upgrades()
        db._run_upgrades()  # Should be a no-op

        from tle.util.db.cache_db_conn import _namedtuple_factory
        db.conn.row_factory = _namedtuple_factory
        version = db.conn.execute(
            'SELECT version FROM cache_db_version LIMIT 1'
        ).fetchone()
        db.conn.row_factory = None
        assert version.version == registry.latest_version


class TestRunUpgradesPreExistingAlias:
    """_run_upgrades on a DB that already has handle_alias (from old code)."""

    def test_detects_pre_upgrade_db_and_clears(self):
        db = _make_db()
        # Simulate pre-upgrade DB with stale data
        db.conn.execute(
            'INSERT INTO handle_alias VALUES (?, ?, ?)',
            ('stale_handle', 'stale_handle', 1000)
        )
        db.conn.commit()

        db._run_upgrades()

        # 1.1.0 should have cleared the table
        count = db.conn.execute('SELECT COUNT(*) FROM handle_alias').fetchone()[0]
        assert count == 0

    def test_already_upgraded_db_no_double_clear(self):
        """After upgrade, restarting should NOT clear newly cached aliases."""
        db = _make_db()
        db._run_upgrades()

        # Add some fresh alias data after upgrade
        now = int(time.time())
        db.conn.execute(
            'INSERT INTO handle_alias VALUES (?, ?, ?)',
            ('Friedrich', 'Friedrich', now)
        )
        db.conn.commit()

        # Simulate restart — run upgrades again
        db._run_upgrades()

        count = db.conn.execute('SELECT COUNT(*) FROM handle_alias').fetchone()[0]
        assert count == 1


class TestFullCacheDbConnInit:
    """Test that CacheDbConn.__init__ runs create_tables + upgrades correctly."""

    def test_init_creates_all_tables(self):
        db = CacheDbConn(':memory:')
        tables = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert 'rating_change' in table_names
        assert 'contest' in table_names
        assert 'handle_alias' in table_names
        assert 'cache_db_version' in table_names

    def test_init_stamps_version(self):
        db = CacheDbConn(':memory:')
        from tle.util.db.cache_db_conn import _namedtuple_factory
        db.conn.row_factory = _namedtuple_factory
        version = db.conn.execute(
            'SELECT version FROM cache_db_version LIMIT 1'
        ).fetchone()
        db.conn.row_factory = None
        assert version.version == registry.latest_version
