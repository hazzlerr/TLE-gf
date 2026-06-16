"""Async migration tests: show-deleted command and alias support."""
import json
import discord
from tests.migrate_test_utils import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
    _FakeGuild, _FakeCtx,
)
from tle.cogs._migrate_helpers import build_fallback_message


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


# =====================================================================
# Alias merge tests
# =====================================================================


class TestAliasSupport:
    """Test alias support: posts keep original emoji, complete resolves aliases."""

    def test_alias_map_roundtrip(self, db):
        """set/get alias_map should round-trip correctly."""
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))
        alias_map = db.get_migration_alias_map(str(GUILD))
        assert alias_map == {CHOC: PILL}

    def test_alias_map_null_returns_empty(self, db):
        """get_migration_alias_map returns {} when alias_map is NULL."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        assert db.get_migration_alias_map(str(GUILD)) == {}

    def test_post_phase_preserves_original_emoji(self, db):
        """Each entry is posted with its original emoji — no conversion."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', CHOC, '445', '100')
        db.update_migration_entry_deleted('111', PILL, json.dumps({'content': f'{PILL} **5** | https://discord.com/channels/{GUILD}/100/111'}))
        db.update_migration_entry_deleted('222', CHOC, json.dumps({'content': f'{CHOC} **3** | https://discord.com/channels/{GUILD}/100/222'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL, CHOC}, db))

        # Two separate messages posted
        assert len(new_channel.sent) == 2
        # First post (msg 111) has pill emoji
        assert PILL in new_channel.sent[0].content
        # Second post (msg 222) has chocolate emoji
        assert CHOC in new_channel.sent[1].content

    def test_post_phase_both_emojis_same_msg_posts_both(self, db):
        """Same original message with both emojis gets two separate posts."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '333', CHOC, '445', '100')
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': f'{PILL} **5** | https://discord.com/channels/{GUILD}/100/333'}))
        db.update_migration_entry_deleted('333', CHOC, json.dumps({'content': f'{CHOC} **3** | https://discord.com/channels/{GUILD}/100/333'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL, CHOC}, db))

        # Both entries posted separately
        assert len(new_channel.sent) == 2
        emojis_in_posts = [new_channel.sent[0].content, new_channel.sent[1].content]
        assert any(PILL in c for c in emojis_in_posts)
        assert any(CHOC in c for c in emojis_in_posts)

    def test_complete_creates_alias_not_config(self, db):
        """Complete should create config for main emoji only, register alias."""
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

        # Pill and chocolate entries for different messages
        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', CHOC, '445', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_crawled('222', CHOC, '500', '778', 3)
        db.update_migration_entry_posted('111', PILL, '888')
        db.update_migration_entry_posted('222', CHOC, '889')
        db.update_migration_status(str(GUILD), 'done')

        # Simulate what complete does
        migration = db.get_migration(str(GUILD))
        alias_map = db.get_migration_alias_map(str(GUILD))
        emojis = migration.emojis.split(',')
        main_emojis = set(emojis) - set(alias_map.keys())
        posted_entries = db.get_posted_migration_entries(str(GUILD))

        seen = set()
        for entry in posted_entries:
            resolved = alias_map.get(entry.emoji, entry.emoji)
            key = (entry.original_msg_id, resolved)
            if key in seen:
                continue
            seen.add(key)
            db.add_starboard_message_v1(
                entry.original_msg_id, entry.new_starboard_msg_id,
                str(GUILD), resolved, author_id=entry.author_id
            )

        for emoji in main_emojis:
            db.add_starboard_emoji(str(GUILD), emoji, 1, 0xffaa10)
            db.set_starboard_channel(str(GUILD), emoji, '200')

        for alias_emoji, main_emoji in alias_map.items():
            db.add_starboard_alias(str(GUILD), alias_emoji, main_emoji)

        # Verify: pill config exists
        pill_cfg = db.get_starboard_entry(str(GUILD), PILL)
        assert pill_cfg is not None
        assert pill_cfg.channel_id == '200'

        # Verify: chocolate has NO config (it's an alias)
        assert db.get_starboard_entry(str(GUILD), CHOC) is None

        # Verify: chocolate is registered as alias of pill
        assert db.resolve_alias(str(GUILD), CHOC) == PILL

        # Verify: both messages stored under pill in starboard_message_v1
        sb1 = db.get_starboard_message_v1('111', PILL)
        sb2 = db.get_starboard_message_v1('222', PILL)
        assert sb1 is not None
        assert sb2 is not None
        # No chocolate entries in starboard_message_v1
        assert db.get_starboard_message_v1('222', CHOC) is None

    def test_complete_deduplicates_same_msg_both_emojis(self, db):
        """When same message has both emojis posted, complete stores only one row."""
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '333', CHOC, '445', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_crawled('333', CHOC, '500', '777', 3)
        db.update_migration_entry_posted('333', PILL, '888')
        db.update_migration_entry_posted('333', CHOC, '889')
        db.update_migration_status(str(GUILD), 'done')

        alias_map = db.get_migration_alias_map(str(GUILD))
        posted_entries = db.get_posted_migration_entries(str(GUILD))

        seen = set()
        imported = 0
        for entry in posted_entries:
            resolved = alias_map.get(entry.emoji, entry.emoji)
            key = (entry.original_msg_id, resolved)
            if key in seen:
                continue
            seen.add(key)
            db.add_starboard_message_v1(
                entry.original_msg_id, entry.new_starboard_msg_id,
                str(GUILD), resolved, author_id=entry.author_id
            )
            imported += 1

        # Only one row stored (de-duplicated)
        assert imported == 1
        assert db.get_starboard_message_v1('333', PILL) is not None

    def test_full_flow_with_aliases(self, db):
        """Full migration: crawl both emojis, post each as-is, both entries tracked."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[
                _FakeReaction(PILL, count=3, user_ids=[10, 11, 12]),
                _FakeReaction(CHOC, count=2, user_ids=[11, 13]),
            ],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])

        pill_bot_msg = _FakeMessage(
            msg_id=1001,
            content=f'{PILL} **3** | https://discord.com/channels/{GUILD}/222/333'
        )
        choc_bot_msg = _FakeMessage(
            msg_id=1002,
            content=f'{CHOC} **2** | https://discord.com/channels/{GUILD}/222/333'
        )
        old_channel = _FakeChannel(channel_id=100, messages=[pill_bot_msg, choc_bot_msg])
        new_channel = _FakeChannel(channel_id=200)

        bot = _FakeBot(channels=[old_channel, source_channel, new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db

        try:
            db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
            db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

            from tle.cogs.migrate import Migrate
            cog = Migrate(bot)
            _run(cog._run_migration(GUILD, 100, 200, {PILL, CHOC}))

            migration = db.get_migration(str(GUILD))
            assert migration.status == 'done'

            # Both entries are posted (one per emoji found on original message)
            pill_entry = db.get_migration_entry('333', PILL)
            choc_entry = db.get_migration_entry('333', CHOC)
            assert pill_entry.crawl_status == 'posted'
            assert choc_entry.crawl_status == 'posted'

            # Both entries have correct star counts from actual reactions
            assert pill_entry.star_count == 3
            assert choc_entry.star_count == 2

            # Reactors stored per emoji
            pill_reactors = db.get_reactors('333', PILL)
            choc_reactors = db.get_reactors('333', CHOC)
            assert set(pill_reactors) == {'10', '11', '12'}
            assert set(choc_reactors) == {'11', '13'}
        finally:
            cf_common.user_db = old_db


