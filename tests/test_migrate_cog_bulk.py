"""DB-level migration cog tests: batched bulk import into starboard tables."""
import json
import pytest
from tests.migrate_test_utils import (
    _FakeMigrateDb, GUILD, PILL, CHOC, db, _zero_rate_delay,
)
from tle.cogs._migrate_helpers import parse_old_bot_message, build_fallback_message
from tle.util.db.user_db_conn import namedtuple_factory
from tle import constants


# =====================================================================
# Bulk import tests (batched raw-SQL complete logic)
# =====================================================================


def _run_complete_import_batched(db, guild_id, new_channel_id):
    """Reproduce the complete phase with batched commits (every 500 rows),
    matching migrate.py lines 494-571 exactly.  The periodic commit ensures
    large datasets don't block the event loop."""
    migration = db.get_migration(str(guild_id))
    emojis = migration.emojis.split(',')
    alias_map = db.get_migration_alias_map(guild_id)
    main_emojis = set(emojis) - set(alias_map.keys()) if alias_map else set(emojis)

    posted_entries = db.get_posted_migration_entries(str(guild_id))

    conn = db.conn
    seen_msgs = set()
    imported = 0
    for i, entry in enumerate(posted_entries):
        resolved_emoji = alias_map.get(entry.emoji, entry.emoji) if alias_map else entry.emoji

        dedup_key = (entry.original_msg_id, resolved_emoji)
        if dedup_key in seen_msgs:
            continue
        seen_msgs.add(dedup_key)

        star_count = entry.star_count or 0
        if alias_map:
            all_family = [resolved_emoji] + [k for k, v in alias_map.items()
                                              if v == resolved_emoji]
            merged_count = db.get_merged_reactor_count(entry.original_msg_id, all_family)
            if merged_count > 0:
                star_count = merged_count

        conn.execute(
            'INSERT OR IGNORE INTO starboard_message_v1 '
            '(original_msg_id, starboard_msg_id, guild_id, emoji, author_id, channel_id) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (str(entry.original_msg_id), str(entry.new_starboard_msg_id),
             str(guild_id), resolved_emoji,
             str(entry.author_id) if entry.author_id else None,
             str(entry.source_channel_id) if entry.source_channel_id else None)
        )
        if star_count > 0:
            conn.execute(
                'UPDATE starboard_message_v1 SET star_count = ? '
                'WHERE original_msg_id = ? AND emoji = ?',
                (star_count, str(entry.original_msg_id), resolved_emoji)
            )
        imported += 1

        # Periodic commit — matches the real code's batching at every 500 rows
        if i % 500 == 499:
            conn.commit()

    for emoji in main_emojis:
        conn.execute(
            'INSERT INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) '
            'VALUES (?, ?, ?, ?) '
            'ON CONFLICT(guild_id, emoji) DO UPDATE SET threshold = excluded.threshold, '
            'color = excluded.color',
            (str(guild_id), emoji, 1, constants._DEFAULT_STAR_COLOR)
        )
        conn.execute(
            'UPDATE starboard_emoji_v1 SET channel_id = ? WHERE guild_id = ? AND emoji = ?',
            (str(new_channel_id), str(guild_id), emoji)
        )

    for alias_emoji, main_emoji in alias_map.items():
        conn.execute(
            'INSERT OR REPLACE INTO starboard_alias (guild_id, alias_emoji, main_emoji) '
            'VALUES (?, ?, ?)',
            (str(guild_id), alias_emoji, main_emoji)
        )

    conn.execute(
        'DELETE FROM starboard_migration_entry WHERE guild_id = ?',
        (str(guild_id),)
    )
    conn.execute(
        'DELETE FROM starboard_migration WHERE guild_id = ?',
        (str(guild_id),)
    )
    conn.commit()

    return imported


class TestCompleteBulkImport:
    """Test that the batched raw-SQL complete logic works for large datasets."""

    def test_complete_bulk_import_1000_entries(self, db):
        """Create 1000 migration entries, run the batched complete import, and verify
        all 1000 are in starboard_message_v1 with correct star_counts and author_ids.
        This exercises the periodic commit at every 500 rows."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        for i in range(1000):
            orig_id = str(10000 + i)
            old_bot_id = str(50000 + i)
            db.add_migration_entry(str(GUILD), orig_id, PILL, old_bot_id, '100')
            db.update_migration_entry_crawled(
                orig_id, PILL, '500', str(70000 + i), i + 1
            )
            db.update_migration_entry_posted(orig_id, PILL, str(80000 + i))

        db.update_migration_status(str(GUILD), 'done')

        imported = _run_complete_import_batched(db, GUILD, '200')
        assert imported == 1000

        # Verify every entry was imported correctly
        for i in range(1000):
            orig_id = str(10000 + i)
            msg = db.get_starboard_message_v1(orig_id, PILL)
            assert msg is not None, f'Missing starboard entry for {orig_id}'
            assert msg.starboard_msg_id == str(80000 + i)
            assert msg.author_id == str(70000 + i)
            assert msg.star_count == i + 1
            assert msg.guild_id == str(GUILD)

        # Migration data should be cleaned up
        assert db.get_migration(str(GUILD)) is None

    def test_complete_bulk_import_preserves_reactors(self, db):
        """Create 100 entries each with 5 reactors, run complete, and verify
        reactor data is still queryable after migration cleanup."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        for i in range(100):
            orig_id = str(10000 + i)
            old_bot_id = str(50000 + i)
            db.add_migration_entry(str(GUILD), orig_id, PILL, old_bot_id, '100')
            db.update_migration_entry_crawled(
                orig_id, PILL, '500', str(70000 + i), 5
            )
            db.update_migration_entry_posted(orig_id, PILL, str(80000 + i))

            # Add 5 reactors per entry
            reactor_ids = [str(90000 + i * 5 + j) for j in range(5)]
            db.bulk_add_reactors(orig_id, PILL, reactor_ids)

        db.update_migration_status(str(GUILD), 'done')

        imported = _run_complete_import_batched(db, GUILD, '200')
        assert imported == 100

        # Verify reactors survive the migration cleanup (they live in
        # starboard_reactors, not starboard_migration_entry)
        for i in range(100):
            orig_id = str(10000 + i)
            reactors = db.get_reactors(orig_id, PILL)
            expected = {str(90000 + i * 5 + j) for j in range(5)}
            assert set(reactors) == expected, (
                f'Reactor mismatch for {orig_id}: got {reactors}'
            )

        # Also verify starboard messages are present with correct star counts
        for i in range(100):
            orig_id = str(10000 + i)
            msg = db.get_starboard_message_v1(orig_id, PILL)
            assert msg is not None
            assert msg.star_count == 5

    def test_complete_bulk_with_aliases_and_dedup(self, db):
        """Create 200 entries: 100 pill + 100 chocolate (alias of pill) for the
        same 100 original messages. Verify dedup produces 100 starboard messages
        with merged star counts."""
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        alias_map = {CHOC: PILL}
        db.set_migration_alias_map(str(GUILD), json.dumps(alias_map))

        for i in range(100):
            orig_id = str(10000 + i)

            # Pill entry
            pill_bot_id = str(50000 + i)
            db.add_migration_entry(str(GUILD), orig_id, PILL, pill_bot_id, '100')
            db.update_migration_entry_crawled(
                orig_id, PILL, '500', str(70000 + i), 3
            )
            db.update_migration_entry_posted(orig_id, PILL, str(80000 + i))

            # Add pill reactors: 3 unique users
            pill_reactor_ids = [str(90000 + i * 10 + j) for j in range(3)]
            db.bulk_add_reactors(orig_id, PILL, pill_reactor_ids)

            # Chocolate entry (same original message, different emoji)
            # Use raw SQL because add_migration_entry would conflict on PK (orig_id, PILL)
            # — here the PK is (orig_id, CHOC) which is distinct.
            choc_bot_id = str(60000 + i)
            db.conn.execute(
                'INSERT OR IGNORE INTO starboard_migration_entry '
                '(guild_id, original_msg_id, emoji, old_bot_msg_id, old_channel_id, '
                'crawl_status, source_channel_id, author_id, star_count, new_starboard_msg_id) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (str(GUILD), orig_id, CHOC, choc_bot_id, '100',
                 'posted', '500', str(70000 + i), 4, str(85000 + i))
            )
            db.conn.commit()

            # Add chocolate reactors: 4 users, 2 overlapping with pill
            # pill has users at offsets 0,1,2; choc has 1,2,3,4
            # distinct union = 0,1,2,3,4 = 5 users
            choc_reactor_ids = [str(90000 + i * 10 + j) for j in range(1, 5)]
            db.bulk_add_reactors(orig_id, CHOC, choc_reactor_ids)

        db.update_migration_status(str(GUILD), 'done')

        imported = _run_complete_import_batched(db, GUILD, '200')

        # Should be 100, not 200 — duplicates for the same original_msg_id
        # with resolved_emoji=PILL are skipped
        assert imported == 100

        # Verify exactly 100 starboard messages exist, all under PILL (resolved)
        count = db.conn.execute(
            'SELECT COUNT(*) as cnt FROM starboard_message_v1 WHERE guild_id = ?',
            (str(GUILD),)
        ).fetchone().cnt
        assert count == 100

        # Verify merged star counts: each message had 3 pill reactors and 4 choc
        # reactors with 2 overlapping, so 5 distinct users each
        for i in range(100):
            orig_id = str(10000 + i)
            msg = db.get_starboard_message_v1(orig_id, PILL)
            assert msg is not None, f'Missing starboard entry for {orig_id}'
            assert msg.star_count == 5, (
                f'Expected merged star_count 5 for {orig_id}, got {msg.star_count}'
            )
            # All entries should be under the resolved emoji (PILL), not CHOC
            assert msg.emoji == PILL

        # No entries under CHOC should exist (all resolved to PILL)
        choc_count = db.conn.execute(
            'SELECT COUNT(*) as cnt FROM starboard_message_v1 WHERE emoji = ?',
            (CHOC,)
        ).fetchone().cnt
        assert choc_count == 0
