"""Async migration tests: crawl phase, post phase, run-migration state machine."""
import json
import discord
from tests.migrate_test_utils import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
    _FakeGuild, _FakeCtx,
)
from tle.cogs._migrate_helpers import build_fallback_message


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

    def test_crawl_fetches_thread_channel(self, db):
        """Messages in threads (not in cache) should be fetched via API fallback."""
        original = _FakeMessage(
            msg_id=333, content='Thread message',
            reactions=[_FakeReaction(PILL, count=2, user_ids=[10, 11])],
            author=_FakeUser(777, 'Author'),
        )
        # Thread channel 222 is NOT passed to _FakeBot's channels list,
        # so get_channel() returns None. But fetch_channel() should find it.
        thread_channel = _FakeChannel(channel_id=222, messages=[original])

        old_bot_msg = self._make_old_bot_msg(1001, PILL, 2, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        # Only old_channel in the cache; thread_channel added for fetch_channel
        bot = _FakeBot(channels=[old_channel])
        # Manually add thread to bot's internal dict so fetch_channel finds it
        bot._channels[222] = thread_channel

        # But simulate get_channel returning None for the thread
        original_get = bot.get_channel
        def get_channel_no_thread(cid):
            if cid == 222:
                return None  # thread not in cache
            return original_get(cid)
        bot.get_channel = get_channel_no_thread

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry is not None
        assert entry.crawl_status == 'crawled'
        assert entry.author_id == '777'
        assert entry.star_count == 2

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
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': f'{PILL} **5** | https://discord.com/channels/{GUILD}/100/333'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 1
        assert PILL in new_channel.sent[0].content

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'posted'

    def test_post_starboard_order(self, db):
        """Posts should be in old starboard order (by old_bot_msg_id), not original msg order."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        # original_msg_id 999 was starboarded FIRST (old_bot_msg_id 444)
        # original_msg_id 111 was starboarded SECOND (old_bot_msg_id 445)
        db.add_migration_entry(str(GUILD), '999', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '111', PILL, '445', '100')
        db.update_migration_entry_deleted('999', PILL, json.dumps({'content': f'{PILL} **3** | https://discord.com/channels/{GUILD}/100/999'}))
        db.update_migration_entry_deleted('111', PILL, json.dumps({'content': f'{PILL} **3** | https://discord.com/channels/{GUILD}/100/111'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 2
        # old_bot_msg_id 444 (original 999) posted first, then 445 (original 111)
        assert '999' in new_channel.sent[0].content
        assert '111' in new_channel.sent[1].content

    def test_post_fetches_thread_channel(self, db):
        """Post phase should use fetch_channel for threads not in cache."""
        original = _FakeMessage(
            msg_id=333, content='Thread message',
            reactions=[_FakeReaction(PILL, count=2, user_ids=[10, 11])],
            author=_FakeUser(777, 'Author'),
        )
        thread_channel = _FakeChannel(channel_id=222, messages=[original])
        new_channel = _FakeChannel(channel_id=200)

        bot = _FakeBot(channels=[new_channel])
        # Thread is reachable via fetch_channel but not get_channel
        bot._channels[222] = thread_channel
        original_get = bot.get_channel
        def get_channel_no_thread(cid):
            if cid == 222:
                return None
            return original_get(cid)
        bot.get_channel = get_channel_no_thread

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '222', '777', 2)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 1
        # Should have used build_starboard_message (not fallback)
        # The content should have the pill emoji and count
        assert PILL in new_channel.sent[0].content

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'posted'

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


