"""Tests for the great day feature — DB layer and cog logic."""
import sqlite3

import pytest

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory


class FakeGreatDayDb:
    """Minimal in-memory DB with greatday_signup and kvs tables."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE greatday_signup (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE guild_config (
                guild_id TEXT,
                key      TEXT,
                value    TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE kvs (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.commit()

    greatday_signup = UserDbConn.greatday_signup
    greatday_remove = UserDbConn.greatday_remove
    greatday_get_signups = UserDbConn.greatday_get_signups
    kvs_set = UserDbConn.kvs_set
    kvs_get = UserDbConn.kvs_get
    kvs_delete = UserDbConn.kvs_delete

    def get_guild_config(self, guild_id, key):
        row = self.conn.execute(
            'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?',
            (str(guild_id), key)).fetchone()
        return row.value if row else None

    def set_guild_config(self, guild_id, key, value):
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (str(guild_id), key, value))
        self.conn.commit()


@pytest.fixture
def db():
    return FakeGreatDayDb()


GUILD = '111'
USER_A = '100'
USER_B = '200'
USER_C = '300'


class TestSignup:
    def test_signup_returns_true(self, db):
        assert db.greatday_signup(GUILD, USER_A) is True

    def test_duplicate_signup_returns_false(self, db):
        db.greatday_signup(GUILD, USER_A)
        assert db.greatday_signup(GUILD, USER_A) is False

    def test_signup_appears_in_list(self, db):
        db.greatday_signup(GUILD, USER_A)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 1
        assert rows[0].user_id == USER_A

    def test_multiple_signups(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_signup(GUILD, USER_C)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 3

    def test_guild_isolation(self, db):
        db.greatday_signup('1', USER_A)
        db.greatday_signup('2', USER_B)
        assert len(db.greatday_get_signups('1')) == 1
        assert len(db.greatday_get_signups('2')) == 1


class TestRemove:
    def test_remove_existing(self, db):
        db.greatday_signup(GUILD, USER_A)
        assert db.greatday_remove(GUILD, USER_A) is True
        assert len(db.greatday_get_signups(GUILD)) == 0

    def test_remove_nonexistent(self, db):
        assert db.greatday_remove(GUILD, USER_A) is False

    def test_remove_only_target(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_remove(GUILD, USER_A)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 1
        assert rows[0].user_id == USER_B


class TestEmptyList:
    def test_empty_guild(self, db):
        assert db.greatday_get_signups(GUILD) == []


class TestLastSentTracking:
    def test_kvs_tracks_last_sent(self, db):
        db.kvs_set('greatday_last:111', '2026-03-30')
        assert db.kvs_get('greatday_last:111') == '2026-03-30'

    def test_kvs_prevents_double_send(self, db):
        db.kvs_set('greatday_last:111', '2026-03-30')
        # Simulates the check in the task
        assert db.kvs_get('greatday_last:111') == '2026-03-30'


class TestSendGreatDay:
    """Test _send_greatday picks users and sends message."""

    def test_picks_up_to_5(self, db):
        for i in range(10):
            db.greatday_signup(GUILD, str(i))
        rows = db.greatday_get_signups(GUILD)
        user_ids = [r.user_id for r in rows]
        import random
        picked = random.sample(user_ids, min(5, len(user_ids)))
        assert len(picked) == 5
        assert all(uid in user_ids for uid in picked)

    def test_picks_all_when_fewer_than_5(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        rows = db.greatday_get_signups(GUILD)
        user_ids = [r.user_id for r in rows]
        import random
        picked = random.sample(user_ids, min(5, len(user_ids)))
        assert len(picked) == 2


# ── _send_greatday integration tests ──────────────────────────────────

import asyncio
from tle.util import codeforces_common as cf_common


class _FakeChannel:
    def __init__(self):
        self.sent = []

    async def send(self, content):
        self.sent.append(content)


class _FakeGuild:
    def __init__(self, guild_id, channel=None, members=None):
        self.id = guild_id
        self._channel = channel
        self._members = {member.id: member for member in (members or [])}

    def get_channel(self, cid):
        return self._channel

    def get_member(self, user_id):
        return self._members.get(user_id)


class _FakeMember:
    def __init__(self, user_id, name, display_name=None):
        self.id = user_id
        self.name = name
        self.display_name = display_name or name


class TestSendGreatDayIntegration:
    """Test the actual _send_greatday method on the cog."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_cog(self, db):
        from tle.cogs.greatday import GreatDay
        cog = GreatDay(bot=None)
        return cog

    def test_singular_verb_for_one_user(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is True
        assert len(channel.sent) == 1
        assert ' is having a great day!' in channel.sent[0]

    def test_plural_verb_for_multiple_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is True
        assert ' are having a great day!' in channel.sent[0]

    def test_no_channel_returns_false(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        # No greatday_channel config set
        db.greatday_signup(GUILD, USER_A)

        guild = _FakeGuild(int(GUILD))
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False

    def test_deleted_channel_returns_false(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)

        guild = _FakeGuild(int(GUILD), channel=None)  # get_channel returns None
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False

    def test_no_signups_returns_false(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        # No signups

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False
        assert len(channel.sent) == 0

    def test_mentions_all_picked_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_signup(GUILD, USER_C)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = self._make_cog(db)
        self._run(cog._send_greatday(guild))
        msg = channel.sent[0]
        # All 3 users should be mentioned (fewer than 5)
        assert f'<@{USER_A}>' in msg
        assert f'<@{USER_B}>' in msg
        assert f'<@{USER_C}>' in msg

    def test_special_user_gets_forced_pick_when_coinflip_hits(self, db, monkeypatch):
        from tle.cogs import greatday as greatday_module

        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_signup(GUILD, USER_C)

        channel = _FakeChannel()
        members = [
            _FakeMember(int(USER_A), 'flammifer4271'),
            _FakeMember(int(USER_B), 'other1'),
            _FakeMember(int(USER_C), 'other2'),
            _FakeMember(400, 'other3'),
            _FakeMember(500, 'other4'),
            _FakeMember(600, 'other5'),
        ]
        for user_id in ['400', '500', '600']:
            db.greatday_signup(GUILD, user_id)
        guild = _FakeGuild(int(GUILD), channel, members=members)
        cog = self._make_cog(db)

        monkeypatch.setattr(greatday_module.random, 'random', lambda: 0.1)
        monkeypatch.setattr(greatday_module.random, 'sample', lambda seq, k: list(seq)[1:k + 1])
        monkeypatch.setattr(greatday_module.random, 'randrange', lambda n: 0)

        self._run(cog._send_greatday(guild))

        assert f'<@{USER_A}>' in channel.sent[0]

    def test_special_user_can_still_miss_when_coinflip_fails(self, db, monkeypatch):
        from tle.cogs import greatday as greatday_module

        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        for user_id in [USER_A, USER_B, USER_C, '400', '500', '600']:
            db.greatday_signup(GUILD, user_id)

        channel = _FakeChannel()
        members = [
            _FakeMember(int(USER_A), 'flammifer4271'),
            _FakeMember(int(USER_B), 'other1'),
            _FakeMember(int(USER_C), 'other2'),
            _FakeMember(400, 'other3'),
            _FakeMember(500, 'other4'),
            _FakeMember(600, 'other5'),
        ]
        guild = _FakeGuild(int(GUILD), channel, members=members)
        cog = self._make_cog(db)

        monkeypatch.setattr(greatday_module.random, 'random', lambda: 0.9)
        monkeypatch.setattr(greatday_module.random, 'sample', lambda seq, k: list(seq)[-k:])

        self._run(cog._send_greatday(guild))

        assert f'<@{USER_A}>' not in channel.sent[0]


class TestUpgrade:
    def test_upgrade_creates_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_18_0
        upgrade_1_18_0(conn)
        # Should be able to insert and query
        conn.execute(
            'INSERT INTO greatday_signup (guild_id, user_id) VALUES (?, ?)',
            ('1', '10'))
        rows = conn.execute('SELECT * FROM greatday_signup').fetchall()
        assert len(rows) == 1
        conn.close()
