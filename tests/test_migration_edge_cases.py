"""Edge-case tests for DB migrations.

Covers multi-guild migration, partial data scenarios, and the
create_tables + upgrade interaction on existing databases.
"""
import sqlite3
from collections import namedtuple

import pytest

from tle.util.db.upgrades import UpgradeRegistry
from tle.util.db.user_db_conn import namedtuple_factory
from tests.test_migrations import make_registry_with_upgrades, create_legacy_tables


@pytest.fixture
def db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    yield conn
    conn.close()


# =====================================================================
# Multi-guild migration
# =====================================================================

class TestMultiGuildMigration:
    def test_two_guilds_different_channels(self, db):
        """Two guilds with different starboard channels migrate correctly."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.execute("INSERT INTO starboard VALUES ('222', '888')")
        db.execute("INSERT INTO starboard_message VALUES ('m1', 's1', '111')")
        db.execute("INSERT INTO starboard_message VALUES ('m2', 's2', '222')")
        db.commit()

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        reg.set_version(db, '1.0.0')
        reg.run(db)

        # Each guild's emoji should have its own channel_id after 1.4.0
        e1 = db.execute(
            "SELECT channel_id FROM starboard_emoji_v1 WHERE guild_id = '111'"
        ).fetchone()
        e2 = db.execute(
            "SELECT channel_id FROM starboard_emoji_v1 WHERE guild_id = '222'"
        ).fetchone()
        assert e1.channel_id == '999'
        assert e2.channel_id == '888'

    def test_guild_with_messages_but_no_config(self, db):
        """Legacy starboard_message references a guild not in starboard table."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        # msg from guild 333 which has no starboard config
        db.execute("INSERT INTO starboard_message VALUES ('m1', 's1', '333')")
        db.commit()

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        reg.set_version(db, '1.0.0')
        reg.run(db)

        # Message should still be migrated to v1
        msg = db.execute(
            "SELECT * FROM starboard_message_v1 WHERE guild_id = '333'"
        ).fetchone()
        assert msg is not None
        assert msg.original_msg_id == 'm1'
        assert msg.emoji == '⭐'


# =====================================================================
# Upgrade 1.1.0 edge cases
# =====================================================================

class TestUpgrade110EdgeCases:
    def test_no_old_tables_at_all(self, db):
        """If the old starboard tables don't exist, migration should not crash."""
        from tle.util.db.user_db_upgrades import upgrade_1_1_0
        # Don't create legacy tables — upgrade should handle the missing table gracefully
        upgrade_1_1_0(db)

        # v1 tables should still be created
        db.execute('SELECT * FROM starboard_config_v1').fetchall()
        db.execute('SELECT * FROM starboard_emoji_v1').fetchall()
        db.execute('SELECT * FROM starboard_message_v1').fetchall()

    def test_duplicate_migration_is_idempotent(self, db):
        """Running 1.1.0 twice doesn't duplicate data."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.execute("INSERT INTO starboard_message VALUES ('m1', 's1', '111')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0
        upgrade_1_1_0(db)
        upgrade_1_1_0(db)  # Second run

        configs = db.execute('SELECT * FROM starboard_config_v1').fetchall()
        emojis = db.execute('SELECT * FROM starboard_emoji_v1').fetchall()
        msgs = db.execute('SELECT * FROM starboard_message_v1').fetchall()
        assert len(configs) == 1
        assert len(emojis) == 1
        assert len(msgs) == 1

    def test_guild_with_null_channel(self, db):
        """Legacy starboard row with NULL channel_id."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', NULL)")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0
        upgrade_1_1_0(db)

        config = db.execute("SELECT * FROM starboard_config_v1").fetchone()
        assert config.channel_id is None


# =====================================================================
# Upgrade 1.4.0 edge cases
# =====================================================================

class TestUpgrade140EdgeCases:
    def test_guild_with_null_channel_in_config(self, db):
        """If starboard_config_v1 has NULL channel_id, don't set it on emoji rows."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', NULL)")
        db.commit()

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        reg.set_version(db, '1.0.0')
        reg.run(db)

        emoji = db.execute(
            "SELECT channel_id FROM starboard_emoji_v1 WHERE guild_id = '111'"
        ).fetchone()
        # channel_id should remain NULL since the config had NULL
        assert emoji.channel_id is None

    def test_guild_with_multiple_emojis_added_between_upgrades(self, db):
        """Guild adds star via migration and fire manually between 1.1.0 and 1.4.0."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        from tle.util.db.user_db_upgrades import (
            upgrade_1_1_0, upgrade_1_2_0, upgrade_1_3_0, upgrade_1_4_0
        )
        upgrade_1_1_0(db)
        upgrade_1_2_0(db)

        # Simulate user adding fire emoji between upgrades
        db.execute(
            "INSERT INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) "
            "VALUES ('111', '🔥', 5, 16711680)"
        )
        db.commit()

        upgrade_1_3_0(db)
        upgrade_1_4_0(db)

        # Both emojis should get the channel from legacy config
        star = db.execute(
            "SELECT channel_id FROM starboard_emoji_v1 WHERE guild_id = '111' AND emoji = '⭐'"
        ).fetchone()
        fire = db.execute(
            "SELECT channel_id FROM starboard_emoji_v1 WHERE guild_id = '111' AND emoji = '🔥'"
        ).fetchone()
        assert star.channel_id == '999'
        assert fire.channel_id == '999'


# =====================================================================
# create_tables + upgrade interaction
# =====================================================================

class TestCreateTablesUpgradeInteraction:
    """Test that create_tables (which creates latest schema) doesn't conflict
    with upgrades that ALTER TABLE or CREATE TABLE IF NOT EXISTS."""

    def test_create_latest_then_run_all_upgrades(self, db):
        """Simulates: create_tables() creates v1 tables WITH all columns,
        then upgrades run. ALTER TABLEs should be no-ops."""
        # Create latest-schema tables (like create_tables() does)
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard (
                guild_id TEXT PRIMARY KEY, channel_id TEXT
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard_message (
                original_msg_id TEXT PRIMARY KEY, starboard_msg_id TEXT, guild_id TEXT
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard_config_v1 (
                guild_id TEXT PRIMARY KEY, channel_id TEXT
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id TEXT, emoji TEXT, threshold INTEGER NOT NULL DEFAULT 3,
                color INTEGER NOT NULL DEFAULT 16755216, channel_id TEXT,
                PRIMARY KEY (guild_id, emoji)
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id TEXT, starboard_msg_id TEXT, guild_id TEXT,
                emoji TEXT, author_id TEXT, star_count INTEGER DEFAULT 0,
                channel_id TEXT, PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id TEXT, key TEXT, value TEXT, PRIMARY KEY (guild_id, key)
            )
        ''')
        db.commit()

        # Now run all upgrades — they should all be no-ops or idempotent
        reg = make_registry_with_upgrades()
        reg.run(db)
        assert reg.get_current_version(db) == '1.4.0'

    def test_resume_from_partial_upgrade(self, db):
        """DB has version 1.2.0 — upgrades 1.3.0 and 1.4.0 should still run."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_2_0
        upgrade_1_1_0(db)
        upgrade_1_2_0(db)

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        reg.set_version(db, '1.2.0')
        reg.run(db)

        assert reg.get_current_version(db) == '1.4.0'
        # 1.3.0 should have added columns
        db.execute(
            "INSERT INTO starboard_message_v1 "
            "(original_msg_id, emoji, guild_id, author_id, star_count, channel_id) "
            "VALUES ('test', '⭐', '111', 'u', 5, '999')"
        )
        db.commit()
        row = db.execute("SELECT * FROM starboard_message_v1 WHERE original_msg_id = 'test'").fetchone()
        assert row.author_id == 'u'
        assert row.star_count == 5
