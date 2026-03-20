"""Async migration tests: crawl phase, post phase, state machine, resume."""
import asyncio
import json
import pytest
import discord
from tests._migrate_fakes import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
)
from tle.cogs._migrate_helpers import build_fallback_message


class _FakeGuild:
    def __init__(self, guild_id=GUILD):
        self.id = guild_id


class _FakeCtx:
    """Minimal ctx for testing command methods directly."""
    def __init__(self, guild_id=GUILD):
        self.guild = _FakeGuild(guild_id)
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append(content)


# =====================================================================
# Async crawl phase tests
# =====================================================================


class TestCrawlPhase:
    """Test _crawl_phase with fake Discord objects."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_crawl_processes_messages(self, db):
        """Crawl should parse old bot messages and create entries."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[10, 11, 12])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])

        old_bot_msg = self._make_old_bot_msg(1001, PILL, 3, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry is not None
        assert entry.crawl_status == 'crawled'
        assert entry.author_id == '777'
        assert entry.star_count == 3

        # Reactors should be recorded
        reactors = db.get_reactors('333', PILL)
        assert len(reactors) == 3

    def test_crawl_skips_wrong_emoji(self, db):
        """Messages with non-target emojis should be skipped."""
        old_bot_msg = self._make_old_bot_msg(1001, '\N{WHITE MEDIUM STAR}', 5, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        bot = _FakeBot(channels=[old_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        assert db.get_migration_entry('333', PILL) is None

    def test_crawl_handles_deleted_original(self, db):
        """When original is not fetchable, entry should be marked 'deleted'."""
        # Old bot message references msg 333 in channel 222, but channel 222 has no messages
        old_bot_msg = self._make_old_bot_msg(1001, PILL, 5, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])
        source_channel = _FakeChannel(channel_id=222, messages=[])  # empty — fetch will fail

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry is not None
        assert entry.crawl_status == 'deleted'

    def test_crawl_resumes_from_checkpoint(self, db):
        """Crawl should resume from the last checkpoint message ID."""
        old_bot_msg1 = self._make_old_bot_msg(1001, PILL, 3, 222, 333)
        old_bot_msg2 = self._make_old_bot_msg(1002, PILL, 5, 222, 444)
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=5, user_ids=[20])],
            author=_FakeUser(888, 'Author2'),
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])
        source_channel = _FakeChannel(channel_id=222, messages=[original2])

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        # Set checkpoint at msg 1001 — should skip it and only process 1002
        db.update_migration_checkpoint(str(GUILD), '1001', 1, 0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        # Should have processed only msg 444 (from old_bot_msg 1002)
        assert db.get_migration_entry('333', PILL) is None  # skipped
        entry = db.get_migration_entry('444', PILL)
        assert entry is not None
        assert entry.crawl_status == 'crawled'

    def test_crawl_channel_not_found(self, db):
        """Crawl should fail gracefully if old channel doesn't exist."""
        bot = _FakeBot(channels=[])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        migration = db.get_migration(str(GUILD))
        assert migration.status == 'failed'


# =====================================================================
# Async post phase tests
# =====================================================================


class TestPostPhase:
    """Test _post_phase with fake Discord objects."""

    def test_post_sends_to_new_channel(self, db):
        """Post phase should send messages to the new channel."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'Hello'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 1
        assert PILL in new_channel.sent[0].content

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'posted'

    def test_post_chronological_order(self, db):
        """Posts should be in snowflake order (oldest first)."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        # Add out of order
        db.add_migration_entry(str(GUILD), '999', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '111', PILL, '445', '100')
        db.update_migration_entry_deleted('999', PILL, json.dumps({'content': 'newer'}))
        db.update_migration_entry_deleted('111', PILL, json.dumps({'content': 'older'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 2
        # First sent should be for msg 111 (older)
        assert '111' in new_channel.sent[0].content
        assert '999' in new_channel.sent[1].content

    def test_post_channel_not_found(self, db):
        """Post should fail gracefully if new channel doesn't exist."""
        bot = _FakeBot(channels=[])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        migration = db.get_migration(str(GUILD))
        assert migration.status == 'failed'

    def test_post_updates_counters(self, db):
        """Post phase should update post_done and post_total."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '444', PILL, '445', '100')
        db.update_migration_entry_deleted('333', PILL, '{}')
        db.update_migration_entry_deleted('444', PILL, '{}')

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        migration = db.get_migration(str(GUILD))
        assert migration.post_total == 2
        assert migration.post_done == 2


# =====================================================================
# _run_migration state machine tests
# =====================================================================


class TestRunMigration:
    """Test the full crawl -> post -> done state machine."""

    def test_full_flow(self, db):
        """Full migration: crawl -> post -> done."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=2, user_ids=[10, 11])],
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

        # Monkeypatch cf_common.user_db
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            # Migration should be done
            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'

            # Entry should be posted
            entry = db.get_migration_entry('333', PILL)
            assert entry.crawl_status == 'posted'

            # New channel should have received a message
            assert len(new_channel.sent) == 1
        finally:
            cf_common.user_db = old_db

    def test_resume_from_posting_skips_crawl(self, db):
        """M3: Resuming a migration in 'posting' status should skip crawl phase."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            # Pre-populate an entry and set status to 'posting'
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'test'}))
            db.update_migration_status(str(GUILD), 'posting')

            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            # Should have posted without crawling
            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'
            assert len(new_channel.sent) == 1
        finally:
            cf_common.user_db = old_db

    def test_error_sets_failed_status(self, db):
        """Errors should set status to 'failed' so ;migrate resume can recover."""
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

            bot = _FakeBot(channels=[])
            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)

            async def fake_crawl(*args, **kwargs):
                raise discord.HTTPException(None, 'server error')

            cog._crawl_phase = fake_crawl
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            migration = db.get_migration(str(GUILD))
            assert migration.status == 'failed'
        finally:
            cf_common.user_db = old_db


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

            # Simulate resume: no postable entries → set to crawling
            postable = db.get_migration_entries_for_posting(str(GUILD))
            assert len(postable) == 0
            db.update_migration_status(str(GUILD), 'crawling')

            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'
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


class TestShowDeletedCommand:
    """Test the ;migrate show-deleted command."""

    def _make_cog(self, db):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.migrate import Migrate
        self._old_db = cf_common.user_db
        cf_common.user_db = db
        bot = _FakeBot()
        return Migrate(bot)

    def _teardown_cog(self):
        from tle.util import codeforces_common as cf_common
        cf_common.user_db = self._old_db

    def _call_show_deleted(self, cog, ctx):
        """Call the underlying async function, bypassing the discord.py stub."""
        _run(cog.show_deleted.__wrapped__(cog, ctx))

    def test_no_migration(self, db):
        """Should report no migration when none exists."""
        cog = self._make_cog(db)
        try:
            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            assert len(ctx.sent) == 1
            assert 'No migration in progress' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_no_deleted_entries(self, db):
        """Should report no deleted messages when all entries are crawled."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_crawled('333', PILL, '500', '777', 5)

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            assert len(ctx.sent) == 1
            assert 'No deleted/inaccessible messages found' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_shows_deleted_entries_with_old_post_links(self, db):
        """Should list deleted entries with links to the old bot's starboard posts."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, '{}')

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            assert len(ctx.sent) == 1
            msg = ctx.sent[0]
            assert 'Deleted/Inaccessible Messages (1)' in msg
            assert f'https://discord.com/channels/{GUILD}/100/444' in msg
            assert PILL in msg
        finally:
            self._teardown_cog()

    def test_shows_new_post_link_after_posting(self, db):
        """After posting, should include a link to the new starboard post too."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, '{}')
            db.update_migration_entry_posted('333', PILL, '888')

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            msg = ctx.sent[0]
            # Old post link
            assert f'https://discord.com/channels/{GUILD}/100/444' in msg
            # New post link (uses migration's new_channel_id=200)
            assert f'https://discord.com/channels/{GUILD}/200/888' in msg
            assert 'Old post' in msg
            assert 'New post' in msg
        finally:
            self._teardown_cog()

    def test_multiple_deleted_entries(self, db):
        """Should list all deleted entries numbered sequentially."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
            db.add_migration_entry(str(GUILD), '333', PILL, '443', '100')
            db.update_migration_entry_deleted('111', PILL, '{}')
            db.update_migration_entry_deleted('222', PILL, '{}')
            db.update_migration_entry_deleted('333', PILL, '{}')

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            msg = ctx.sent[0]
            assert 'Deleted/Inaccessible Messages (3)' in msg
            assert '1.' in msg
            assert '2.' in msg
            assert '3.' in msg
        finally:
            self._teardown_cog()

    def test_mixed_deleted_and_crawled(self, db):
        """Only deleted entries should appear, not crawled ones."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
            db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
            db.update_migration_entry_deleted('222', PILL, '{}')

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            msg = ctx.sent[0]
            assert 'Deleted/Inaccessible Messages (1)' in msg
            # Only msg 222's old bot post should appear
            assert '442' in msg
            assert '441' not in msg
        finally:
            self._teardown_cog()

    def test_multiple_emojis_deleted(self, db):
        """Same original message deleted for different emojis should show both."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.add_migration_entry(str(GUILD), '333', CHOC, '445', '100')
            db.update_migration_entry_deleted('333', PILL, '{}')
            db.update_migration_entry_deleted('333', CHOC, '{}')

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            msg = ctx.sent[0]
            assert 'Deleted/Inaccessible Messages (2)' in msg
            assert PILL in msg
            assert CHOC in msg
        finally:
            self._teardown_cog()

    def test_pagination_splits_long_output(self, db):
        """Should send multiple messages when output exceeds Discord's char limit."""
        # Use a realistic-length guild ID so links are long enough to trigger pagination
        long_guild = 111222333444555666
        cog = self._make_cog(db)
        try:
            old_ch = '100200300400500600'
            db.create_migration(str(long_guild), old_ch, '200300400500600700', PILL, 1000.0)
            for i in range(25):
                msg_id = str(900100200300400000 + i)
                bot_msg_id = str(800100200300400000 + i)
                db.add_migration_entry(str(long_guild), msg_id, PILL, bot_msg_id, old_ch)
                db.update_migration_entry_deleted(msg_id, PILL, '{}')

            ctx = _FakeCtx(guild_id=long_guild)
            self._call_show_deleted(cog, ctx)
            # Should have sent multiple messages
            assert len(ctx.sent) > 1
            # All messages should be within Discord's limit
            for msg in ctx.sent:
                assert len(msg) <= 1900
            # All 25 entries should be accounted for
            all_text = '\n'.join(ctx.sent)
            assert '25.' in all_text
        finally:
            self._teardown_cog()

    def test_chronological_order(self, db):
        """Entries should be listed oldest-first (by snowflake ID)."""
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            # Add out of order
            db.add_migration_entry(str(GUILD), '999', PILL, '449', '100')
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.update_migration_entry_deleted('999', PILL, '{}')
            db.update_migration_entry_deleted('111', PILL, '{}')

            ctx = _FakeCtx()
            self._call_show_deleted(cog, ctx)
            msg = ctx.sent[0]
            # 111 should appear before 999 (as entry 1 vs entry 2)
            pos_111 = msg.index('441')  # old_bot_msg_id for 111
            pos_999 = msg.index('449')  # old_bot_msg_id for 999
            assert pos_111 < pos_999
        finally:
            self._teardown_cog()
