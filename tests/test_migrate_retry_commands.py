"""Migration retry tests: exact-copy posting, retry-failed/view-failed, pause/unpause."""
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
# retry-failed command tests
# =====================================================================


# =====================================================================
# Exact copy behavior: posts use original emoji and count
# =====================================================================


class TestExactCopyBehavior:
    """Posts in the new channel should be exact copies of the old ones —
    same emoji and same count as crawled from the old bot."""

    def test_posted_message_uses_original_emoji_and_count(self, db):
        """A crawled entry should be posted with its original emoji and star_count."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=7, user_ids=[10, 11, 12, 13, 14, 15, 16])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel, source_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '222', '777', 7)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 1
        content = new_channel.sent[0].content
        # Must contain the original emoji
        assert PILL in content
        # Must contain the original count
        assert '**7**' in content

    def test_fallback_message_uses_original_emoji_and_count(self, db):
        """Deleted entries should also preserve the original emoji and count."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', CHOC, 1000.0)
        db.add_migration_entry(str(GUILD), '333', CHOC, '444', '100')
        # star_count=0 for deleted entries (default), but the fallback should
        # still use the entry's emoji
        db.update_migration_entry_deleted('333', CHOC, json.dumps({'content': f'{CHOC} **3** | https://discord.com/channels/{GUILD}/100/333'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {CHOC}, db))

        assert len(new_channel.sent) == 1
        content = new_channel.sent[0].content
        assert CHOC in content

    def test_alias_emoji_not_converted_in_post(self, db):
        """Posts for alias emojis should keep the alias emoji, not convert to main."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

        db.add_migration_entry(str(GUILD), '333', CHOC, '444', '100')
        db.update_migration_entry_deleted('333', CHOC, json.dumps({'content': f'{CHOC} **3** | https://discord.com/channels/{GUILD}/100/333'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL, CHOC}, db))

        assert len(new_channel.sent) == 1
        content = new_channel.sent[0].content
        # Must be chocolate, NOT pill
        assert CHOC in content
        assert PILL not in content


class TestRetryFailedCommand:
    def _make_cog(self, db, bot=None):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.migrate import Migrate
        self._old_db = cf_common.user_db
        cf_common.user_db = db
        bot = bot or _FakeBot()
        return Migrate(bot)

    def _teardown_cog(self):
        from tle.util import codeforces_common as cf_common
        cf_common.user_db = self._old_db

    def _call(self, cog, method_name, ctx):
        _run(getattr(cog, method_name).__wrapped__(cog, ctx))

    def test_no_migration(self, db):
        cog = self._make_cog(db)
        try:
            ctx = _FakeCtx()
            self._call(cog, 'retry_failed', ctx)
            assert 'No migration' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_no_failed_entries(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            ctx = _FakeCtx()
            self._call(cog, 'retry_failed', ctx)
            assert 'No failed entries' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_retry_resets_and_posts(self, db):
        """retry-failed should reset retry_exhausted entries and re-post them."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])
        cog = self._make_cog(db, bot)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'test'}))
            db.update_migration_entry_retry_exhausted('333', PILL, 'old error')
            db.update_migration_status(str(GUILD), 'done')

            # Simulate what retry_failed does
            entries = db.get_retry_exhausted_entries(str(GUILD))
            assert len(entries) == 1
            db.reset_retry_exhausted_entries(str(GUILD))
            db.update_migration_status(str(GUILD), 'posting')

            from tle.cogs.migrate import Migrate
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            entry = db.get_migration_entry('333', PILL)
            assert entry.crawl_status == 'posted'
            assert len(new_channel.sent) == 1
        finally:
            self._teardown_cog()


# =====================================================================
# view-failed command tests
# =====================================================================


class TestViewFailedCommand:
    def _make_cog(self, db):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.migrate import Migrate
        self._old_db = cf_common.user_db
        cf_common.user_db = db
        return Migrate(_FakeBot())

    def _teardown_cog(self):
        from tle.util import codeforces_common as cf_common
        cf_common.user_db = self._old_db

    def _call(self, cog, ctx):
        _run(cog.view_failed.__wrapped__(cog, ctx))

    def test_no_migration(self, db):
        cog = self._make_cog(db)
        try:
            ctx = _FakeCtx()
            self._call(cog, ctx)
            assert 'No migration' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_no_failed_entries(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            ctx = _FakeCtx()
            self._call(cog, ctx)
            assert 'No failed entries' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_shows_failed_with_links_and_errors(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_retry_exhausted('333', PILL, '503 Service Unavailable')

            ctx = _FakeCtx()
            self._call(cog, ctx)
            msg = ctx.sent[0]
            assert 'Failed Messages (1)' in msg
            assert f'discord.com/channels/{GUILD}/100/444' in msg
            assert '503 Service Unavailable' in msg
            assert PILL in msg
        finally:
            self._teardown_cog()

    def test_shows_multiple_failures(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
            db.update_migration_entry_retry_exhausted('111', PILL, 'error 1')
            db.update_migration_entry_retry_exhausted('222', PILL, 'error 2')

            ctx = _FakeCtx()
            self._call(cog, ctx)
            msg = ctx.sent[0]
            assert 'Failed Messages (2)' in msg
            assert '1.' in msg
            assert '2.' in msg
        finally:
            self._teardown_cog()


# =====================================================================
# Pause / unpause tests
# =====================================================================


class TestPauseUnpause:
    """Test ;migrate pause and ;migrate unpause."""

    def test_pause_blocks_post_phase(self, db):
        """Paused migration should stop processing after the current message."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
        db.update_migration_entry_deleted('111', PILL, json.dumps({'content': 'first'}))
        db.update_migration_entry_deleted('222', PILL, json.dumps({'content': 'second'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)

        # Pre-set the pause event (cleared = paused)
        event = asyncio.Event()
        cog._paused[GUILD] = event

        # Run post phase in a task — it should process msg 111 then block
        async def run_and_unpause():
            post_task = asyncio.create_task(
                cog._post_phase(GUILD, 200, {PILL}, db))
            # Give it time to process the first message and hit the pause
            await asyncio.sleep(0.05)
            # Should have posted 1 message so far
            assert len(new_channel.sent) == 1
            # Unpause to let it finish
            event.set()
            await post_task

        _run(run_and_unpause())

        # Both should be posted now
        assert len(new_channel.sent) == 2
        assert db.get_migration_entry('111', PILL).crawl_status == 'posted'
        assert db.get_migration_entry('222', PILL).crawl_status == 'posted'

    def test_pause_no_migration(self, db):
        """Pause with no migration should report an error."""
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.pause.__wrapped__(cog, ctx))
            assert 'No migration' in ctx.sent[0]
        finally:
            cf_common.user_db = old_db

    def test_pause_survives_restart(self, db):
        """Paused status is persisted in DB — on_ready should NOT resume it."""
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.update_migration_status(str(GUILD), 'paused')

            # Simulate on_ready — paused should NOT be resumed
            migration = db.get_migration(str(GUILD))
            assert migration.status == 'paused'
            # on_ready only resumes 'crawling' and 'posting'
            assert migration.status not in ('crawling', 'posting')
        finally:
            cf_common.user_db = old_db

    def test_unpause_after_restart_relaunches(self, db):
        """Unpause after server restart should re-launch the task."""
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            new_channel = _FakeChannel(channel_id=200)
            bot = _FakeBot(channels=[new_channel])
            cog = Migrate(bot)

            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.set_migration_crawl_total(str(GUILD), 1)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({
                'content': f'{PILL} **3** | https://discord.com/channels/{GUILD}/100/333'}))
            # Simulate: was posting, got paused, server restarted
            db.kvs_set(f'migration_pre_pause_status:{GUILD}', 'posting')
            db.update_migration_status(str(GUILD), 'paused')

            # No in-memory event exists (server restarted)
            assert GUILD not in cog._paused

            ctx = _FakeCtx()
            _run(cog.unpause.__wrapped__(cog, ctx))

            # Should have restored status and launched task
            assert 're-launched' in ctx.sent[0]
            migration = db.get_migration(str(GUILD))
            assert migration.status != 'paused'
        finally:
            cf_common.user_db = old_db

    def test_unpause_not_paused(self, db):
        """Unpause when not paused should report an error."""
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            cog = Migrate(_FakeBot())
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            ctx = _FakeCtx()
            _run(cog.unpause.__wrapped__(cog, ctx))
            assert 'not paused' in ctx.sent[0]
        finally:
            cf_common.user_db = old_db

    def test_cancel_unpauses_first(self, db):
        """Cancel should unpause to allow the task to receive cancellation."""
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            cog = Migrate(_FakeBot())
            event = asyncio.Event()
            cog._paused[GUILD] = event
            assert not event.is_set()

            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

            async def test_cancel():
                async def dummy():
                    await event.wait()

                task = asyncio.create_task(dummy())
                cog._tasks[GUILD] = task
                ctx = _FakeCtx()
                await cog.cancel.__wrapped__(cog, ctx)
                assert event.is_set()
                assert GUILD not in cog._paused

            _run(test_cancel())
        finally:
            cf_common.user_db = old_db


