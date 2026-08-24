"""Tests for great day signup/signout event tracking, stats and backfill."""
import asyncio

import pytest

from tle.cogs import _greatday_events as events
from tle.util import codeforces_common as cf_common

from tests.greatday_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, FakeGreatDayDb, db,
)


class _Author:
    def __init__(self, user_id, display_name='someone'):
        self.id = int(user_id)
        self.display_name = display_name
        self.mention = f'<@{user_id}>'


class _Msg:
    def __init__(self, content, author=None, msg_id=1, at=0.0, embeds=()):
        self.content = content
        self.author = author
        self.id = msg_id
        self.embeds = list(embeds)

        class _Created:
            def timestamp(_self):
                return at
        self.created_at = _Created()


class _Embed:
    def __init__(self, description='', title=''):
        self.description = description
        self.title = title


class _Ctx:
    def __init__(self, guild, author, message):
        self.guild = guild
        self.author = author
        self.message = message
        self.sent = []

    async def send(self, content=None, embed=None, **kwargs):
        self.sent.append(embed if embed is not None else content)
        return _Msg('', msg_id=999)


class _Guild:
    def __init__(self, guild_id):
        self.id = int(guild_id)


class _HistoryChannel:
    def __init__(self, messages):
        # Callers pass oldest-first; the scan reads newest-first.
        self._messages = list(messages)

    def history(self, limit=None, oldest_first=False):
        ordered = (self._messages if oldest_first
                   else list(reversed(self._messages)))

        async def _gen():
            for msg in ordered:
                yield msg
        return _gen()


BOT_ID = 7
SIGNUP_OK = _Embed('You have been signed up for great day pings!')
SIGNOUT_OK = _Embed('You have been removed from great day pings.')
ALREADY = _Embed('You are already signed up.')


def _bot_reply(embed, msg_id):
    return _Msg('', author=_Author(BOT_ID), msg_id=msg_id, embeds=[embed])


class TestEventDb:
    def test_records_and_reads_events_newest_first(self, db):
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_record_signup_event(GUILD, USER_A, 'signout', 200.0, 2)
        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [(r.action, r.at) for r in rows] == [
            ('signout', 200.0), ('signup', 100.0)]

    def test_recording_is_idempotent_per_message(self, db):
        assert db.greatday_record_signup_event(
            GUILD, USER_A, 'signup', 100.0, 1) is True
        assert db.greatday_record_signup_event(
            GUILD, USER_A, 'signup', 100.0, 1) is False
        assert len(db.greatday_get_signup_events(GUILD, USER_A)) == 1

    def test_unknown_action_rejected(self, db):
        with pytest.raises(ValueError):
            db.greatday_record_signup_event(GUILD, USER_A, 'banned', 1.0, 1)

    def test_last_signup_ignores_signouts(self, db):
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_record_signup_event(GUILD, USER_A, 'signout', 300.0, 2)
        assert db.greatday_get_last_signup(GUILD, USER_A).at == 100.0

    def test_last_signup_none_without_events(self, db):
        assert db.greatday_get_last_signup(GUILD, USER_A) is None

    def test_events_are_per_guild_and_user(self, db):
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        assert db.greatday_get_signup_events(GUILD, USER_B) == []
        assert db.greatday_get_signup_events('999', USER_A) == []

    def test_post_times_collapse_multi_user_picks(self, db):
        db.greatday_record_picks(GUILD, [USER_A, USER_B], 10, 100.0)
        db.greatday_record_picks(GUILD, [USER_A], 11, 200.0)
        assert db.greatday_get_post_times(GUILD) == [100.0, 200.0]


class TestSignedUpPostCount:
    def _events(self, *pairs):
        return [_Row(action, at) for action, at in pairs]

    def test_counts_posts_inside_membership(self):
        rows = self._events(('signup', 100.0), ('signout', 300.0))
        count, complete = events.signed_up_post_count(
            rows, [50.0, 150.0, 250.0, 400.0], currently_signed_up=False)
        assert (count, complete) == (2, True)

    def test_open_interval_counts_later_posts(self):
        rows = self._events(('signup', 100.0))
        count, complete = events.signed_up_post_count(
            rows, [50.0, 150.0, 9e9], currently_signed_up=True)
        assert (count, complete) == (2, True)

    def test_signout_without_signup_is_incomplete(self):
        rows = self._events(('signout', 300.0))
        count, complete = events.signed_up_post_count(
            rows, [100.0, 400.0], currently_signed_up=False)
        assert (count, complete) == (0, False)

    def test_member_without_events_is_incomplete(self):
        count, complete = events.signed_up_post_count(
            [], [100.0], currently_signed_up=True)
        assert (count, complete) == (0, False)

    def test_never_signed_up_is_zero_and_complete(self):
        count, complete = events.signed_up_post_count(
            [], [100.0], currently_signed_up=False)
        assert (count, complete) == (0, True)


class _Row:
    """Stand-in for a signup event row."""

    _next_id = 0

    def __init__(self, action, at):
        self.action = action
        self.at = at
        type(self)._next_id += 1
        self.message_id = str(type(self)._next_id)


class _Pick:
    def __init__(self, picked_at, message_id):
        self.picked_at = picked_at
        self.message_id = str(message_id)


class TestParseSignupCommand:
    @pytest.mark.parametrize('content,expected', [
        (';greatday signup', ('signup', '100')),
        (';greatday remove', ('signout', '100')),
        ('  ;greatday  SIGNUP ', ('signup', '100')),
        ('<@7> greatday signup', ('signup', '100')),
        (';greatday add <@200>', ('signup', '200')),
        (';greatday kick <@!200>', ('signout', '200')),
    ])
    def test_parses_membership_commands(self, content, expected):
        msg = _Msg(content, author=_Author('100'))
        assert events.parse_signup_command(msg) == expected

    @pytest.mark.parametrize('content', [
        ';greatday stats', ';greatday ban <@200>', 'greatday signups',
        'I hope <@100> is having a great day!', '', ';greatday add',
    ])
    def test_ignores_other_messages(self, content):
        msg = _Msg(content, author=_Author('100'))
        assert events.parse_signup_command(msg) is None


class TestScanSignupEvents:
    def _scan(self, messages):
        channel = _HistoryChannel(messages)
        return asyncio.run(events.scan_signup_events(channel, GUILD, BOT_ID))

    def test_records_confirmed_commands(self):
        msgs = [
            _Msg(';greatday signup', _Author(USER_A), msg_id=1, at=10.0),
            _bot_reply(SIGNUP_OK, 2),
            _Msg(';greatday remove', _Author(USER_A), msg_id=3, at=30.0),
            _bot_reply(SIGNOUT_OK, 4),
        ]
        scanned, found = self._scan(msgs)
        assert scanned == 4
        assert sorted(found) == sorted([
            (GUILD, USER_A, 'signup', 10.0, 1),
            (GUILD, USER_A, 'signout', 30.0, 3),
        ])

    def test_skips_commands_the_bot_rejected(self):
        msgs = [
            _Msg(';greatday signup', _Author(USER_A), msg_id=1, at=10.0),
            _bot_reply(ALREADY, 2),
        ]
        assert self._scan(msgs) == (2, [])

    def test_skips_replies_from_other_users(self):
        impostor = _Msg('', author=_Author('42'), msg_id=2,
                        embeds=[SIGNUP_OK])
        msgs = [_Msg(';greatday signup', _Author(USER_A), msg_id=1, at=10.0),
                impostor]
        assert self._scan(msgs) == (2, [])

    def test_tolerates_chatter_between_command_and_reply(self):
        msgs = [
            _Msg(';greatday signup', _Author(USER_A), msg_id=1, at=10.0),
            _Msg('hi', _Author('42'), msg_id=2),
            _bot_reply(SIGNUP_OK, 3),
        ]
        _, found = self._scan(msgs)
        assert found == [(GUILD, USER_A, 'signup', 10.0, 1)]

    def test_scan_result_is_idempotent_when_stored_twice(self, db):
        msgs = [
            _Msg(';greatday signup', _Author(USER_A), msg_id=1, at=10.0),
            _bot_reply(SIGNUP_OK, 2),
        ]
        _, found = self._scan(msgs)
        assert db.greatday_record_signup_events(found) == 1
        assert db.greatday_record_signup_events(found) == 0

    def test_reports_progress(self):
        msgs = [_Msg('hi', _Author('42'), msg_id=i) for i in range(4)]
        seen = []

        async def progress(scanned, matched):
            seen.append((scanned, matched))

        channel = _HistoryChannel(msgs)
        asyncio.run(events.scan_signup_events(
            channel, GUILD, BOT_ID, progress=progress, progress_interval=2))
        assert seen == [(2, 0), (4, 0)]


class TestCollapseEvents:
    def test_drops_repeated_signups(self):
        rows = [_Row('signup', 100.0), _Row('signup', 200.0),
                _Row('signout', 300.0), _Row('signout', 400.0),
                _Row('signup', 500.0)]
        kept = events.collapse_events(rows)
        assert [(row.action, row.at) for row in kept] == [
            ('signup', 500.0), ('signout', 300.0), ('signup', 100.0)]

    def test_accepts_newest_first_input(self):
        rows = [_Row('signup', 200.0), _Row('signup', 100.0)]
        kept = events.collapse_events(reversed(rows))
        assert [(row.action, row.at) for row in kept] == [('signup', 100.0)]

    def test_empty(self):
        assert events.collapse_events([]) == []


class TestMergeHistory:
    def test_interleaves_picks_and_events_newest_first(self):
        picks = [_Pick(200.0, 20)]
        rows = [_Row('signup', 100.0), _Row('signout', 300.0)]
        assert events.merge_history(picks, rows) == [
            'Signed out — <t:300:F> (<t:300:R>)',
            '<t:200:F> (<t:200:R>)',
            'Signed up — <t:100:F> (<t:100:R>)',
        ]

    def test_empty_when_nothing_recorded(self):
        assert events.merge_history([], []) == []


def _make_cog():
    from tle.cogs.greatday import GreatDay
    return GreatDay(bot=None)


class TestCommandsRecordEvents:
    def _ctx(self, author_id, msg_id, at):
        author = _Author(author_id)
        return _Ctx(_Guild(GUILD), author,
                    _Msg(';greatday signup', author, msg_id=msg_id, at=at))

    def test_signup_records_event(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        ctx = self._ctx(USER_A, 5, 50.0)
        asyncio.run(cog.signup.callback(cog, ctx))
        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [(r.action, r.at) for r in rows] == [('signup', 50.0)]

    def test_repeated_signup_records_nothing_new(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        asyncio.run(cog.signup.callback(cog, self._ctx(USER_A, 5, 50.0)))
        asyncio.run(cog.signup.callback(cog, self._ctx(USER_A, 6, 60.0)))
        assert len(db.greatday_get_signup_events(GUILD, USER_A)) == 1

    def test_remove_records_signout(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        asyncio.run(cog.signup.callback(cog, self._ctx(USER_A, 5, 50.0)))
        asyncio.run(cog.remove.callback(cog, self._ctx(USER_A, 6, 60.0)))
        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [(r.action, r.at) for r in rows] == [
            ('signout', 60.0), ('signup', 50.0)]

    def test_remove_without_signup_records_nothing(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        asyncio.run(cog.remove.callback(cog, self._ctx(USER_A, 6, 60.0)))
        assert db.greatday_get_signup_events(GUILD, USER_A) == []

    def test_admin_add_and_kick_record_target_events(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        admin = _Author('999')
        member = _Author(USER_B, 'target')
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday add', admin, msg_id=11, at=110.0))
        asyncio.run(cog.add_user.callback(cog, ctx, member))
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday kick', admin, msg_id=12, at=120.0))
        asyncio.run(cog.kick_user.callback(cog, ctx, member))
        rows = db.greatday_get_signup_events(GUILD, USER_B)
        assert [(r.action, r.at) for r in rows] == [
            ('signout', 120.0), ('signup', 110.0)]

    def test_ban_records_signout_only_when_signed_up(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        admin = _Author('999')
        member = _Author(USER_B, 'target')
        db.greatday_signup(GUILD, USER_B)
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday ban', admin, msg_id=13, at=130.0))
        asyncio.run(cog.ban_user.callback(cog, ctx, member))
        rows = db.greatday_get_signup_events(GUILD, USER_B)
        assert [(r.action, r.at) for r in rows] == [('signout', 130.0)]

        db.greatday_unban(GUILD, USER_B)
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday ban', admin, msg_id=14, at=140.0))
        asyncio.run(cog.ban_user.callback(cog, ctx, member))
        assert len(db.greatday_get_signup_events(GUILD, USER_B)) == 1

    def test_signup_survives_event_logging_failure(self, db, monkeypatch):
        """Membership already changed, so a logging error must not surface."""
        monkeypatch.setattr(cf_common, 'user_db', db)

        def _raise(*a, **kw):
            raise RuntimeError('simulated DB failure')
        monkeypatch.setattr(db, 'greatday_record_signup_event', _raise)
        cog = _make_cog()
        ctx = self._ctx(USER_A, 5, 50.0)
        asyncio.run(cog.signup.callback(cog, ctx))
        assert db.greatday_is_signed_up(GUILD, USER_A)
        assert len(ctx.sent) == 1


class TestMemberStats:
    def test_reports_last_signup_and_days(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_signup(GUILD, USER_A)
        db.greatday_record_picks(GUILD, [USER_A], 10, 150.0)
        db.greatday_record_picks(GUILD, [USER_B], 11, 250.0)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'great-day\'d **1** time(s)' in text
        assert 'Last signup: <t:100:F>' in text
        assert 'Days signed up: **2**' in text
        assert 'at least' not in text

    def test_marks_unrecorded_signup(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.greatday_signup(GUILD, USER_A)
        db.greatday_record_picks(GUILD, [USER_A], 10, 150.0)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'Last signup: not recorded' in text
        assert 'Days signed up: **0** (at least' in text

    def test_marks_absent_member(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'not recorded (not signed up)' in text
        assert 'Days signed up: **0**' in text
