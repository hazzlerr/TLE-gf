"""Shared helpers for the (split) DB upgrade-registry test modules.

The ``db`` fixture and the legacy-table / registry builders used by
``test_migrations*.py``. NOT a test file (no ``test_`` prefix), so pytest won't
collect it.
"""
import sqlite3

import pytest

from tle.util.db.upgrades import UpgradeRegistry
from tle.util.db.user_db_conn import namedtuple_factory


@pytest.fixture
def db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    yield conn
    conn.close()


def make_registry_with_upgrades():
    """Build a fresh registry and register all the upgrade functions.

    We re-import them fresh to avoid polluting the module-level registry.
    """
    # We can't re-register on the module-level registry, so we replicate
    # the upgrade functions here against a fresh UpgradeRegistry.
    from tle.util.db.user_db_upgrades import (
        upgrade_1_0_0, upgrade_1_1_0, upgrade_1_2_0,
        upgrade_1_3_0, upgrade_1_4_0,
    )
    reg = UpgradeRegistry(version_table='db_version')
    reg.upgrades = [
        ('1.0.0', 'Baseline', upgrade_1_0_0),
        ('1.1.0', 'Multi-emoji starboard', upgrade_1_1_0),
        ('1.2.0', 'Guild config system', upgrade_1_2_0),
        ('1.3.0', 'Star count and author tracking', upgrade_1_3_0),
        ('1.4.0', 'Per-emoji starboard channels', upgrade_1_4_0),
    ]
    return reg


def create_legacy_tables(db):
    """Create the old-style starboard tables (pre-upgrade schema)."""
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard (
            guild_id    TEXT PRIMARY KEY,
            channel_id  TEXT
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_message (
            original_msg_id    TEXT PRIMARY KEY,
            starboard_msg_id   TEXT,
            guild_id           TEXT
        )
    ''')
    db.commit()
