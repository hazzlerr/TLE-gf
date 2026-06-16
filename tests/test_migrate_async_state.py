"""Async migration tests: posted-entry queries, post_failed, resume, complete warnings."""
import json
import discord
from tests.migrate_test_utils import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
    _FakeGuild, _FakeCtx,
)
from tle.cogs._migrate_helpers import build_fallback_message


# =====================================================================
# DB method: get_posted_migration_entries
# =====================================================================


class TestGetPostedMigrationEntries:
    def test_returns_only_posted(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')
        db.add_migration_entry(str(GUILD), '333', PILL, '446', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_deleted('222', PILL, '{}')
        db.update_migration_entry_crawled('333', PILL, '500', '778', 3)
        db.update_migration_entry_posted('333', PILL, '888')

        posted = db.get_posted_migration_entries(str(GUILD))
        assert len(posted) == 1
        assert posted[0].original_msg_id == '333'
        assert posted[0].crawl_status == 'posted'

    def test_chronological_order(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '999', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '111', PILL, '445', '100')
        db.update_migration_entry_crawled('999', PILL, '500', '777', 3)
        db.update_migration_entry_crawled('111', PILL, '500', '778', 7)
        db.update_migration_entry_posted('999', PILL, '888')
        db.update_migration_entry_posted('111', PILL, '889')

        posted = db.get_posted_migration_entries(str(GUILD))
        assert len(posted) == 2
        assert posted[0].original_msg_id == '111'
        assert posted[1].original_msg_id == '999'

    def test_empty_when_none_posted(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        posted = db.get_posted_migration_entries(str(GUILD))
        assert len(posted) == 0


# =====================================================================
# post_failed status tests
# =====================================================================


class TestPostFailedStatus:
    """Test that post_failed entries are handled correctly."""

    def test_post_failed_excluded_from_posting(self, db):
        """post_failed entries should NOT be picked up by get_migration_entries_for_posting."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_crawled('222', PILL, '500', '778', 3)
        db.update_migration_entry_post_failed('111', PILL)

        entries = db.get_migration_entries_for_posting(str(GUILD))
        assert len(entries) == 1
        assert entries[0].original_msg_id == '222'

    def test_post_failed_preserves_data(self, db):
        """post_failed should not clear source_channel_id, author_id, or star_count."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_post_failed('333', PILL)

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'post_failed'
        assert entry.source_channel_id == '500'
        assert entry.author_id == '777'
        assert entry.star_count == 5

    def test_post_failed_shown_in_status_counts(self, db):
        """count_migration_entries_by_status should include post_failed."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_post_failed('111', PILL)
        db.update_migration_entry_crawled('222', PILL, '500', '778', 3)
        db.update_migration_entry_posted('222', PILL, '888')

        counts = db.count_migration_entries_by_status(str(GUILD))
        by_status = {r.crawl_status: r.cnt for r in counts}
        assert by_status.get('post_failed') == 1
        assert by_status.get('posted') == 1

    def test_reset_post_failed_with_source_channel(self, db):
        """Entries with source_channel_id should reset to 'crawled'."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_post_failed('333', PILL)

        db.reset_post_failed_entries(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'crawled'
        assert entry.source_channel_id == '500'

    def test_reset_post_failed_without_source_channel(self, db):
        """Entries without source_channel_id should reset to 'deleted'."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, '{}')
        db.update_migration_entry_post_failed('333', PILL)

        db.reset_post_failed_entries(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'


# =====================================================================
# Resume command logic tests
# =====================================================================


class TestResumeLogic:
    """Test the resume flow for failed migrations."""

    def test_resume_resets_post_failed_and_posts(self, db):
        """Resume should reset post_failed entries and re-post them."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'test'}))
            db.update_migration_entry_post_failed('333', PILL)
            db.update_migration_status(str(GUILD), 'failed')

            # Simulate what ;migrate resume does
            db.reset_post_failed_entries(str(GUILD))
            postable = db.get_migration_entries_for_posting(str(GUILD))
            assert len(postable) == 1
            db.update_migration_status(str(GUILD), 'posting')

            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'
            assert len(new_channel.sent) == 1

            entry = db.get_migration_entry('333', PILL)
            assert entry.crawl_status == 'posted'
        finally:
            cf_common.user_db = old_db

    def test_resume_from_failed_crawl(self, db):
        """Resume from a failed crawl should re-crawl from checkpoint."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=2, user_ids=[10])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])
        old_bot_msg = _FakeMessage(
            msg_id=1001,
            content=f'{PILL} **2** | https://discord.com/channels/{GUILD}/222/333'
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])
        new_channel = _FakeChannel(channel_id=200)

        bot = _FakeBot(channels=[old_channel, source_channel, new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.update_migration_status(str(GUILD), 'failed')
            # crawl_total=0 means crawl never finished
            db.update_migration_status(str(GUILD), 'crawling')

            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'
        finally:
            cf_common.user_db = old_db

    def test_resume_failed_crawl_does_not_skip_to_posting(self, db):
        """Bug fix: if crawl failed mid-way with entries already crawled,
        resume must NOT jump to posting — it must finish crawling first."""
        original1 = _FakeMessage(
            msg_id=333, content='First',
            reactions=[_FakeReaction(PILL, count=2, user_ids=[10])],
            author=_FakeUser(777, 'Author'),
        )
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[20, 21])],
            author=_FakeUser(888, 'Author2'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original1, original2])

        old_bot_msg1 = _FakeMessage(
            msg_id=1001,
            content=f'{PILL} **2** | https://discord.com/channels/{GUILD}/222/333'
        )
        old_bot_msg2 = _FakeMessage(
            msg_id=1002,
            content=f'{PILL} **3** | https://discord.com/channels/{GUILD}/222/444'
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])
        new_channel = _FakeChannel(channel_id=200)

        bot = _FakeBot(channels=[old_channel, source_channel, new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            # Simulate: crawl processed msg 333 then crashed (503 on msg 444)
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '1001', '100')
            db.update_migration_entry_crawled('333', PILL, '222', '777', 2)
            db.update_migration_checkpoint(str(GUILD), '1001', 1, 0)
            # crawl_total=0 because crawl never finished
            db.update_migration_status(str(GUILD), 'failed')

            migration = db.get_migration(str(GUILD))
            assert migration.crawl_total == 0  # crawl didn't finish

            # Resume should detect incomplete crawl and continue crawling
            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)

            # Simulate what resume command does
            db.reset_post_failed_entries(str(GUILD))
            if migration.status == 'posting' or (migration.status == 'failed' and migration.crawl_total > 0):
                db.update_migration_status(str(GUILD), 'posting')
            else:
                db.update_migration_status(str(GUILD), 'crawling')

            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            # Should have crawled msg 444 (from checkpoint) and then posted both
            entry444 = db.get_migration_entry('444', PILL)
            assert entry444 is not None
            assert entry444.crawl_status == 'posted'

            entry333 = db.get_migration_entry('333', PILL)
            assert entry333.crawl_status == 'posted'

            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'

            # Both messages posted
            assert len(new_channel.sent) == 2
        finally:
            cf_common.user_db = old_db


# =====================================================================
# Complete warning tests
# =====================================================================


class TestCompleteWarning:
    """Test that complete warns about post_failed entries."""

    def test_complete_with_post_failed_shows_count(self, db):
        """count_migration_entries_by_status should reveal post_failed for the warning."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_posted('111', PILL, '888')
        db.update_migration_entry_crawled('222', PILL, '500', '778', 3)
        db.update_migration_entry_post_failed('222', PILL)
        db.update_migration_status(str(GUILD), 'done')

        counts = db.count_migration_entries_by_status(str(GUILD))
        by_status = {r.crawl_status: r.cnt for r in counts}
        assert by_status.get('post_failed') == 1
        assert by_status.get('posted') == 1

    def test_complete_without_post_failed_no_warning(self, db):
        """No post_failed entries should mean no warning needed."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_posted('333', PILL, '888')
        db.update_migration_status(str(GUILD), 'done')

        counts = db.count_migration_entries_by_status(str(GUILD))
        by_status = {r.crawl_status: r.cnt for r in counts}
        assert by_status.get('post_failed', 0) == 0


# =====================================================================
# ;migrate show-deleted command tests
# =====================================================================


