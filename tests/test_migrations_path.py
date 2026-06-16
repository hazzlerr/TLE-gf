"""Tests for the DB upgrade functions: full path + per-version checks (1.1.0-1.4.0)."""
import pytest

from tests.migrations_test_utils import (
    db, make_registry_with_upgrades, create_legacy_tables,
)


# =====================================================================
# Full upgrade path from legacy data
# =====================================================================

class TestFullUpgradePath:
    def test_upgrade_from_legacy_with_data(self, db):
        """Simulate a pre-upgrade DB with starboard data, run all upgrades."""
        create_legacy_tables(db)

        # Insert legacy data
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.execute("INSERT INTO starboard VALUES ('222', '888')")
        db.execute("INSERT INTO starboard_message VALUES ('msg1', 'sb1', '111')")
        db.execute("INSERT INTO starboard_message VALUES ('msg2', 'sb2', '111')")
        db.execute("INSERT INTO starboard_message VALUES ('msg3', 'sb3', '222')")
        db.commit()

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        reg.set_version(db, '1.0.0')  # Simulate pre-upgrade detection
        reg.run(db)

        # Should be at latest version
        assert reg.get_current_version(db) == '1.4.0'

        # starboard_config_v1 should have the legacy channels
        rows = db.execute('SELECT * FROM starboard_config_v1 ORDER BY guild_id').fetchall()
        assert len(rows) == 2
        assert rows[0].guild_id == '111'
        assert rows[0].channel_id == '999'

        # starboard_emoji_v1 should have default star emoji for each guild
        star = '⭐'
        emojis = db.execute('SELECT * FROM starboard_emoji_v1 ORDER BY guild_id').fetchall()
        assert len(emojis) == 2
        assert emojis[0].emoji == star
        assert emojis[0].threshold == 3
        # 1.4.0 should have migrated channel_id
        assert emojis[0].channel_id == '999'
        assert emojis[1].channel_id == '888'

        # starboard_message_v1 should have all 3 messages
        msgs = db.execute('SELECT * FROM starboard_message_v1 ORDER BY original_msg_id').fetchall()
        assert len(msgs) == 3
        assert msgs[0].original_msg_id == 'msg1'
        assert msgs[0].emoji == star
        # 1.3.0 columns should exist
        assert hasattr(msgs[0], 'author_id')
        assert hasattr(msgs[0], 'star_count')
        assert hasattr(msgs[0], 'channel_id')

        # guild_config table should exist
        db.execute('SELECT * FROM guild_config').fetchall()  # No error = table exists

    def test_upgrade_from_empty_legacy(self, db):
        """Legacy tables exist but are empty."""
        create_legacy_tables(db)

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        reg.set_version(db, '1.0.0')
        reg.run(db)

        assert reg.get_current_version(db) == '1.4.0'
        # Tables should exist, just empty
        assert db.execute('SELECT COUNT(*) as cnt FROM starboard_emoji_v1').fetchone().cnt == 0

    def test_upgrade_from_scratch_no_legacy(self, db):
        """Run all upgrades on a completely empty DB (no legacy tables).
        1.1.0's migration should gracefully handle missing old tables."""
        reg = make_registry_with_upgrades()
        reg.run(db)

        assert reg.get_current_version(db) == '1.4.0'
        # v1 tables should have been created
        db.execute('SELECT * FROM starboard_emoji_v1').fetchall()
        db.execute('SELECT * FROM starboard_message_v1').fetchall()
        db.execute('SELECT * FROM guild_config').fetchall()


# =====================================================================
# Individual upgrade tests
# =====================================================================

class TestUpgrade110:
    def test_creates_v1_tables(self, db):
        create_legacy_tables(db)
        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)

        # Run just 1.0.0 and 1.1.0
        from tle.util.db.user_db_upgrades import upgrade_1_0_0, upgrade_1_1_0
        upgrade_1_0_0(db)
        upgrade_1_1_0(db)

        # Tables should exist
        db.execute('SELECT * FROM starboard_config_v1').fetchall()
        db.execute('SELECT * FROM starboard_emoji_v1').fetchall()
        db.execute('SELECT * FROM starboard_message_v1').fetchall()

    def test_migrates_config_data(self, db):
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0
        upgrade_1_1_0(db)

        row = db.execute('SELECT * FROM starboard_config_v1').fetchone()
        assert row.guild_id == '111'
        assert row.channel_id == '999'

        emoji_row = db.execute('SELECT * FROM starboard_emoji_v1').fetchone()
        assert emoji_row.guild_id == '111'
        assert emoji_row.emoji == '⭐'

    def test_migrates_message_data(self, db):
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.execute("INSERT INTO starboard_message VALUES ('msg1', 'sb1', '111')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0
        upgrade_1_1_0(db)

        row = db.execute('SELECT * FROM starboard_message_v1').fetchone()
        assert row.original_msg_id == 'msg1'
        assert row.starboard_msg_id == 'sb1'
        assert row.guild_id == '111'
        assert row.emoji == '⭐'


class TestUpgrade130:
    def test_adds_columns(self, db):
        create_legacy_tables(db)
        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_3_0
        upgrade_1_1_0(db)
        upgrade_1_3_0(db)

        # Insert a row and verify new columns work
        db.execute(
            "INSERT INTO starboard_message_v1 "
            "(original_msg_id, starboard_msg_id, guild_id, emoji, author_id, star_count, channel_id) "
            "VALUES ('msg1', 'sb1', '111', '⭐', 'user1', 5, '999')"
        )
        db.commit()
        row = db.execute('SELECT * FROM starboard_message_v1').fetchone()
        assert row.author_id == 'user1'
        assert row.star_count == 5
        assert row.channel_id == '999'

    def test_idempotent(self, db):
        """Running 1.3.0 twice should not crash (columns already exist)."""
        create_legacy_tables(db)
        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_3_0
        upgrade_1_1_0(db)
        upgrade_1_3_0(db)
        upgrade_1_3_0(db)  # Should not raise


class TestUpgrade140:
    def test_migrates_channel_id_to_emoji_rows(self, db):
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_3_0, upgrade_1_4_0
        upgrade_1_1_0(db)
        upgrade_1_3_0(db)
        upgrade_1_4_0(db)

        row = db.execute('SELECT * FROM starboard_emoji_v1').fetchone()
        assert row.channel_id == '999'

    def test_multiple_emojis_get_same_channel(self, db):
        """If a guild had one channel and we add multiple emojis, all get the channel."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_3_0, upgrade_1_4_0
        upgrade_1_1_0(db)
        # Manually add a second emoji (simulating an add between 1.1.0 and 1.4.0)
        db.execute(
            "INSERT INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) "
            "VALUES ('111', '🔥', 5, 16711680)"
        )
        db.commit()
        upgrade_1_3_0(db)
        upgrade_1_4_0(db)

        rows = db.execute(
            'SELECT emoji, channel_id FROM starboard_emoji_v1 ORDER BY emoji'
        ).fetchall()
        assert len(rows) == 2
        # Both should have channel_id from the legacy config
        for row in rows:
            assert row.channel_id == '999'

    def test_does_not_overwrite_existing_channel(self, db):
        """1.4.0 only sets channel_id WHERE channel_id IS NULL."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_3_0, upgrade_1_4_0
        upgrade_1_1_0(db)
        upgrade_1_3_0(db)

        # Pre-set a different channel on the star emoji before 1.4.0 runs
        # (simulate: 1.4.0 adds the column, then we set it, then the UPDATE runs)
        # Actually, the ALTER TABLE happens first, then the UPDATE.
        # Let's just test the UPDATE logic by adding the column ourselves.
        try:
            db.execute('ALTER TABLE starboard_emoji_v1 ADD COLUMN channel_id TEXT')
        except Exception:
            pass  # Already exists from 1.1.0 in some flows
        db.execute(
            "UPDATE starboard_emoji_v1 SET channel_id = '777' WHERE guild_id = '111'"
        )
        db.commit()

        upgrade_1_4_0(db)  # Should NOT overwrite 777 with 999

        row = db.execute(
            "SELECT channel_id FROM starboard_emoji_v1 WHERE guild_id = '111'"
        ).fetchone()
        assert row.channel_id == '777'  # Preserved, not overwritten

    def test_idempotent(self, db):
        """Running 1.4.0 twice should not crash."""
        create_legacy_tables(db)
        from tle.util.db.user_db_upgrades import upgrade_1_1_0, upgrade_1_3_0, upgrade_1_4_0
        upgrade_1_1_0(db)
        upgrade_1_3_0(db)
        upgrade_1_4_0(db)
        upgrade_1_4_0(db)  # Should not raise


