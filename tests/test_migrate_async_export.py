"""Async migration tests: crawl ignores display emoji, pillboard export command."""
import json
import discord
from tests.migrate_test_utils import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
    _FakeGuild, _FakeCtx,
)
from tle.cogs._migrate_helpers import build_fallback_message


# =====================================================================
# Tests verifying crawl ignores old bot's display emoji
# =====================================================================

CATSHOCK = ':catshock:'


class TestCrawlIgnoresDisplayEmoji:
    """Verify the crawl phase scans original message reactions, not the old bot's display emoji."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_display_catshock_original_has_pill(self, db):
        """Old bot displays :catshock: but original has pill reactions -- pill entry crawled."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=4, user_ids=[10, 11, 12, 13])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])

        old_bot_msg = self._make_old_bot_msg(1001, CATSHOCK, 4, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        # Pill entry should exist with correct data from the original's reaction
        pill_entry = db.get_migration_entry('333', PILL)
        assert pill_entry is not None
        assert pill_entry.crawl_status == 'crawled'
        assert pill_entry.author_id == '777'
        assert pill_entry.star_count == 4

        # Reactors should be recorded
        reactors = db.get_reactors('333', PILL)
        assert set(reactors) == {'10', '11', '12', '13'}

        # No entry for catshock itself (it's not in emoji_set)
        catshock_entry = db.get_migration_entry('333', CATSHOCK)
        assert catshock_entry is None

    def test_display_catshock_original_has_pill_and_choc(self, db):
        """Old bot displays :catshock: but original has both pill AND chocolate reactions."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[
                _FakeReaction(PILL, count=3, user_ids=[10, 11, 12]),
                _FakeReaction(CHOC, count=2, user_ids=[20, 21]),
            ],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])

        old_bot_msg = self._make_old_bot_msg(1001, CATSHOCK, 5, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL, CHOC}, db))

        # Both pill and chocolate entries should be crawled
        pill_entry = db.get_migration_entry('333', PILL)
        assert pill_entry is not None
        assert pill_entry.crawl_status == 'crawled'
        assert pill_entry.star_count == 3

        choc_entry = db.get_migration_entry('333', CHOC)
        assert choc_entry is not None
        assert choc_entry.crawl_status == 'crawled'
        assert choc_entry.star_count == 2

        # Reactors per emoji
        pill_reactors = db.get_reactors('333', PILL)
        assert set(pill_reactors) == {'10', '11', '12'}
        choc_reactors = db.get_reactors('333', CHOC)
        assert set(choc_reactors) == {'20', '21'}

    def test_display_pill_original_no_matching_reactions(self, db):
        """Old bot displays pill but original has no matching reactions —
        should still be crawled using the displayed count from the old bot header."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction('\N{THUMBS UP SIGN}', count=5, user_ids=[10, 11, 12, 13, 14])],
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

        # Entry should be crawled with the displayed count (3) from old bot header
        entry = db.get_migration_entry('333', PILL)
        assert entry is not None
        assert entry.crawl_status == 'crawled'
        assert entry.star_count == 3
        assert entry.author_id == '777'

    def test_two_old_bot_msgs_same_original_no_duplicates(self, db):
        """Two old bot messages (pill and catshock) reference the same original -- no duplicate entries."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[10, 11, 12])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])

        # Two different old bot messages referencing the same original msg 333
        pill_bot_msg = self._make_old_bot_msg(1001, PILL, 3, 222, 333)
        catshock_bot_msg = self._make_old_bot_msg(1002, CATSHOCK, 3, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[pill_bot_msg, catshock_bot_msg])

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        # Only one pill entry should exist (idempotent via INSERT OR IGNORE)
        entry = db.get_migration_entry('333', PILL)
        assert entry is not None
        assert entry.crawl_status == 'crawled'
        assert entry.star_count == 3

        # Verify only one entry total for this original message
        rows = db.conn.execute(
            'SELECT COUNT(*) AS cnt FROM starboard_migration_entry WHERE original_msg_id = ?',
            ('333',)
        ).fetchone()
        assert rows.cnt == 1


class TestPillboardExportCommand:
    """Test the ;pillboard-export command."""

    def test_export_fetches_original_messages(self, monkeypatch, tmp_path):
        from tle.cogs import migrate as migrate_mod
        from tle.cogs.migrate import Migrate

        monkeypatch.setattr(migrate_mod, '_EXPORT_DIR', tmp_path)

        guild = _FakeGuild(GUILD)
        context_msg_1 = _FakeMessage(
            msg_id=331,
            content='first context message',
            author=_FakeUser(775, 'Context1'),
        )
        context_msg_2 = _FakeMessage(
            msg_id=332,
            content='second context message',
            author=_FakeUser(776, 'Context2'),
        )
        original = _FakeMessage(
            msg_id=333,
            content='original message text',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[10, 11, 12])],
            author=_FakeUser(777, 'Author'),
        )
        old_bot_msg = _FakeMessage(
            msg_id=1001,
            content=(
                f'{PILL} **3** | '
                f'https://discord.com/channels/{GUILD}/222/333'
            ),
            author=_FakeUser(123, 'OldBot'),
        )
        source_channel = _FakeChannel(
            channel_id=222,
            messages=[context_msg_1, context_msg_2, original],
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])
        context_msg_1.channel = source_channel
        context_msg_1.guild = guild
        context_msg_2.channel = source_channel
        context_msg_2.guild = guild
        original.channel = source_channel
        original.guild = guild
        old_bot_msg.channel = old_channel
        old_bot_msg.guild = guild

        bot = _FakeBot(channels=[old_channel, source_channel])
        cog = Migrate(bot)

        class _ExportCtx:
            def __init__(self):
                self.guild = _FakeGuild(GUILD)
                self.guild.filesize_limit = 50 * 1024 * 1024
                self.sent = []

            async def send(self, content=None, **kwargs):
                self.sent.append((content, kwargs))

        ctx = _ExportCtx()
        _run(cog.pillboard_export.__wrapped__(
            cog, ctx, old_channel, '+context=2', PILL))

        exported = list(tmp_path.glob('pillboard_export_*.json'))
        assert len(exported) == 1
        payload = json.loads(exported[0].read_text())
        assert payload['summary']['scanned'] == 1
        assert payload['summary']['parsed'] == 1
        assert payload['summary']['fetched'] == 1
        assert payload['summary']['failed'] == 0
        assert payload['summary']['context_fetched'] == 1
        assert payload['summary']['context_failed'] == 0
        assert payload['context_limit'] == 2
        assert payload['messages'][0]['pillboard']['displayed_count'] == 3
        assert payload['messages'][0]['original']['content'] == (
            'original message text')
        assert payload['messages'][0]['original']['author']['id'] == '777'
        assert [
            row['content'] for row in payload['messages'][0]['context_before']
        ] == ['first context message', 'second context message']
        assert 'file' in ctx.sent[-1][1]
