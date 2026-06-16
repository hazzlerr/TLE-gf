"""Migration retry tests: fetch-channel exhaustion, resume, reactor/generic failures, restart-post."""
import asyncio
import json
import pytest

import discord
from tests.migrate_test_utils import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
    _FakeGuild, _FakeCtx, _zero_retry_delay,
)
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError


# =====================================================================
# Bug fix verification tests
# =====================================================================


class TestFetchSourceChannelRetryExhausted:
    """Verify _fetch_source_channel RetryExhaustedError is caught in crawl."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_crawl_catches_fetch_channel_retry_exhausted(self, db):
        """If fetch_channel fails persistently, entry is retry_exhausted, crawl continues."""
        old_bot_msg1 = self._make_old_bot_msg(1001, PILL, 5, 222, 333)
        old_bot_msg2 = self._make_old_bot_msg(1002, PILL, 3, 223, 444)
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[20])],
            author=_FakeUser(888, 'Author2'),
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])
        source_channel2 = _FakeChannel(channel_id=223, messages=[original2])

        bot = _FakeBot(channels=[old_channel, source_channel2])
        # Channel 222 is NOT in bot — fetch_channel will raise NotFound,
        # but we override it to always raise HTTPException for channel 222
        real_fetch_channel = bot.fetch_channel

        async def flaky_fetch_channel(cid):
            if cid == 222:
                raise discord.HTTPException(None, 'persistent failure')
            return await real_fetch_channel(cid)

        bot.fetch_channel = flaky_fetch_channel

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        # 333 should be retry_exhausted (channel 222 unreachable)
        entry333 = db.get_migration_entry('333', PILL)
        assert entry333.crawl_status == 'retry_exhausted'
        assert 'persistent failure' in entry333.last_error

        # 444 should be crawled (channel 223 works)
        entry444 = db.get_migration_entry('444', PILL)
        assert entry444.crawl_status == 'crawled'

    def test_post_catches_fetch_channel_retry_exhausted(self, db):
        """If fetch_channel fails in post phase, falls through to fallback."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])
        # Channel 222 not in bot — fetch_channel raises HTTPException
        real_fetch_channel = bot.fetch_channel

        async def always_fail_fetch(cid):
            if cid == 222:
                raise discord.HTTPException(None, 'persistent failure')
            return await real_fetch_channel(cid)

        bot.fetch_channel = always_fail_fetch

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '222', '777', 5)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        # Should still post (using fallback), not crash
        entry = db.get_migration_entry('333', PILL)
        # Either posted (fallback worked) or retry_exhausted (send also failed)
        # Since new_channel.send works, it should be posted
        assert entry.crawl_status == 'posted'
        assert len(new_channel.sent) == 1


class TestResumeResetsRetryExhausted:
    """Verify resume now resets retry_exhausted entries (bug #4 fix)."""

    def test_resume_resets_retry_exhausted_entries(self, db):
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            new_channel = _FakeChannel(channel_id=200)
            bot = _FakeBot(channels=[new_channel])

            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'test'}))
            db.update_migration_entry_retry_exhausted('333', PILL, 'old error')
            # Crawl finished (crawl_total > 0), then post failed
            db.set_migration_crawl_total(str(GUILD), 1)
            db.update_migration_status(str(GUILD), 'failed')

            # Simulate what resume does
            db.reset_post_failed_entries(str(GUILD))
            db.reset_retry_exhausted_entries(str(GUILD))

            # Entry should be reset to 'deleted' (no source_channel_id)
            entry = db.get_migration_entry('333', PILL)
            assert entry.crawl_status == 'deleted'
            assert entry.last_error is None

            # Now it should be postable
            postable = db.get_migration_entries_for_posting(str(GUILD))
            assert len(postable) == 1
        finally:
            cf_common.user_db = old_db


class TestCompleteWarningRetryExhausted:
    """Verify complete warns about retry_exhausted entries (coverage gap #8)."""

    def test_complete_warns_about_retry_exhausted(self, db):
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')
            db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
            db.update_migration_entry_posted('111', PILL, '888')
            db.update_migration_entry_retry_exhausted('222', PILL, 'bad error')
            db.update_migration_status(str(GUILD), 'done')

            from tle.cogs.migrate import Migrate
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.complete.__wrapped__(cog, ctx, type('Ch', (), {
                'id': 200, 'mention': '#new'})()))

            # Should warn about the 1 failed entry
            assert any('1 entries failed' in msg for msg in ctx.sent)
        finally:
            cf_common.user_db = old_db

    def test_complete_no_posted_entries_gives_feedback(self, db):
        """Complete with 0 posted entries should not return silently."""
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_retry_exhausted('333', PILL, 'error')
            db.update_migration_status(str(GUILD), 'done')

            from tle.cogs.migrate import Migrate
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.complete.__wrapped__(cog, ctx, type('Ch', (), {
                'id': 200, 'mention': '#new'})()))

            # Should have warning + "no posted entries" message
            all_msgs = ' '.join(ctx.sent)
            assert 'retry-failed' in all_msgs
            assert 'No posted entries' in all_msgs
        finally:
            cf_common.user_db = old_db


class TestReactorFetchFailure:
    """Verify reactor fetch failure falls back to displayed count (gap #9)."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_reactor_fetch_failure_uses_displayed_count(self, db):
        """If reaction.users() fails, star_count should fall back to displayed_count."""

        class _FailingReaction:
            def __init__(self, emoji_str, count):
                self.emoji = emoji_str
                self.count = count

            async def users(self):
                raise discord.HTTPException(None, 'reactor fetch failed')
                yield  # make it an async generator

        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FailingReaction(PILL, 7)],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])
        old_bot_msg = self._make_old_bot_msg(1001, PILL, 7, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])
        bot = _FakeBot(channels=[old_channel, source_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'crawled'
        # Should use the displayed count (7) from the old bot message
        assert entry.star_count == 7


class TestGenericExceptionInPostPhase:
    """Verify non-Discord exceptions in post phase mark retry_exhausted (gap #10)."""

    def test_non_discord_exception_marks_retry_exhausted(self, db):
        new_channel = _FakeChannel(channel_id=200)

        async def exploding_send(**kwargs):
            raise ValueError('unexpected internal error')

        new_channel.send = lambda **kw: exploding_send(**kw)

        bot = _FakeBot(channels=[new_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'hi'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'retry_exhausted'
        assert 'unexpected internal error' in entry.last_error


class TestPauseCrawlPhase:
    """Verify pause works during crawl phase (gap #7)."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_pause_blocks_crawl_phase(self, db):
        original1 = _FakeMessage(
            msg_id=333, content='First',
            reactions=[_FakeReaction(PILL, count=1, user_ids=[10])],
            author=_FakeUser(777, 'Author'),
        )
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=1, user_ids=[20])],
            author=_FakeUser(888, 'Author2'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original1, original2])
        old_bot_msg1 = self._make_old_bot_msg(1001, PILL, 1, 222, 333)
        old_bot_msg2 = self._make_old_bot_msg(1002, PILL, 1, 222, 444)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])

        bot = _FakeBot(channels=[old_channel, source_channel])

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        event = asyncio.Event()
        cog._paused[GUILD] = event

        async def run_and_unpause():
            crawl_task = asyncio.create_task(
                cog._crawl_phase(GUILD, 100, {PILL}, db))
            await asyncio.sleep(0.05)
            # Should have crawled first message then paused
            entry333 = db.get_migration_entry('333', PILL)
            assert entry333 is not None
            assert entry333.crawl_status == 'crawled'
            # Second message should NOT be crawled yet
            entry444 = db.get_migration_entry('444', PILL)
            assert entry444 is None
            # Unpause
            event.set()
            await crawl_task

        _run(run_and_unpause())

        # Both should be crawled now
        assert db.get_migration_entry('333', PILL).crawl_status == 'crawled'
        assert db.get_migration_entry('444', PILL).crawl_status == 'crawled'


# =====================================================================
# restart-post tests
# =====================================================================


class TestRestartPost:
    """Test ;migrate restart-post command."""

    def test_restart_post_deletes_and_reposts(self, db):
        """restart-post should delete posted messages and re-run post phase."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.set_migration_crawl_total(str(GUILD), 2)

            # Simulate two entries that were already posted
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
            db.update_migration_entry_crawled('111', PILL, '500', '777', 3)
            db.update_migration_entry_crawled('222', PILL, '500', '778', 5)
            db.update_migration_entry_posted('111', PILL, '901')
            db.update_migration_entry_posted('222', PILL, '902')
            db.update_migration_status(str(GUILD), 'done')

            # Put the "posted" messages in the new channel so they can be fetched+deleted
            posted1 = _FakeMessage(msg_id=901, content=f'{PILL} **3**')
            posted2 = _FakeMessage(msg_id=902, content=f'{PILL} **5**')
            new_channel._messages[901] = posted1
            new_channel._messages[902] = posted2

            # Simulate what restart_post does (without running the background task)
            msg_ids = db.get_all_posted_msg_ids(str(GUILD))
            assert set(msg_ids) == {'901', '902'}

            db.reset_all_entries_for_repost(str(GUILD))

            # Entries should be back to crawled
            e1 = db.get_migration_entry('111', PILL)
            e2 = db.get_migration_entry('222', PILL)
            assert e1.crawl_status == 'crawled'
            assert e2.crawl_status == 'crawled'
            assert e1.new_starboard_msg_id is None
            assert e2.new_starboard_msg_id is None

            # They should be postable again
            entries = db.get_migration_entries_for_posting(str(GUILD))
            assert len(entries) == 2
        finally:
            cf_common.user_db = old_db

    def test_restart_post_resets_retry_exhausted_too(self, db):
        """restart-post should also reset retry_exhausted entries."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_retry_exhausted('333', PILL, 'old error')

        db.reset_all_entries_for_repost(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'  # no source_channel_id
        assert entry.last_error is None
        assert entry.new_starboard_msg_id is None

    def test_restart_post_resets_deleted_entries(self, db):
        """Deleted entries with embed_fallback should remain postable after reset."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        fb = json.dumps({'content': f'{PILL} **3** | https://discord.com/channels/{GUILD}/100/333'})
        db.update_migration_entry_deleted('333', PILL, fb)
        db.update_migration_entry_posted('333', PILL, '901')

        db.reset_all_entries_for_repost(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'
        assert entry.new_starboard_msg_id is None
        # embed_fallback should be preserved
        assert entry.embed_fallback == fb

    def test_restart_post_no_migration(self, db):
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.restart_post.__wrapped__(cog, ctx))
            assert 'No migration' in ctx.sent[0]
        finally:
            cf_common.user_db = old_db

    def test_restart_post_cancels_running_task(self, db):
        """restart-post should cancel a running/paused task automatically."""
        new_channel = _FakeChannel(channel_id=200)
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            cog = Migrate(_FakeBot(channels=[new_channel]))

            async def test():
                # Simulate a paused task
                event = asyncio.Event()
                cog._paused[GUILD] = event
                dummy_task = asyncio.create_task(event.wait())
                cog._tasks[GUILD] = dummy_task

                ctx = _FakeCtx()
                await cog.restart_post.__wrapped__(cog, ctx)

                # Task should have been cancelled
                assert dummy_task.done()
                assert GUILD not in cog._paused
                # Should have proceeded (not blocked)
                assert any('Deleting' in msg for msg in ctx.sent)

            _run(test())
        finally:
            cf_common.user_db = old_db
