"""Startup history reconciliation tests for configured counting threads."""

import asyncio
import logging

import pytest

from tests.counting_test_utils import (
    FakeAuthor,
    FakeGuild,
    FakeMessage,
    FakeThreadChannel,
)
from tle.cogs.counting import Counting
from tle.util import codeforces_common as cf_common
from tle.util.db.user_db_conn import UserDbConn


@pytest.fixture
def db(monkeypatch):
    database = UserDbConn(':memory:')
    monkeypatch.setattr(cf_common, 'user_db', database)
    try:
        yield database
    finally:
        database.conn.close()


def _run(coro):
    return asyncio.run(coro)


def _message(message_id, content, channel, guild, *, offset=0):
    return FakeMessage(
        message_id,
        content,
        channel,
        guild=guild,
        author=FakeAuthor(10, 'Alice'),
        offset=offset,
    )


class FakeBot:
    def __init__(self, *, cached=None, fetched=None):
        self.cached = dict(cached or {})
        self.fetched = dict(fetched or {})
        self.fetch_calls = []
        self.closed = False

    def get_channel(self, channel_id):
        return self.cached.get(channel_id)

    async def fetch_channel(self, channel_id):
        self.fetch_calls.append(channel_id)
        value = self.fetched.get(channel_id)
        if isinstance(value, Exception):
            raise value
        return value

    def is_closed(self):
        return self.closed


class FailingHistoryChannel(FakeThreadChannel):
    def history(self, **kwargs):
        self.history_calls.append(kwargs)
        raise RuntimeError('history unavailable')


def _thread(channel_id, guild, contents):
    channel = FakeThreadChannel(channel_id=channel_id)
    channel.guild = guild
    channel.messages = [
        _message(index, content, channel, guild, offset=index)
        for index, content in enumerate(contents, start=1)
    ]
    return channel


class TestCountingStartupReparse:
    def test_cached_thread_is_fully_reparsed_once_and_keeps_configuration(
            self, db):
        guild = FakeGuild()
        thread = _thread(200, guild, ['1', '10', '3'])
        db.counting_configure(
            guild.id,
            thread.id,
            current_count=77,
            last_message_id=999,
            configured_by=321,
            now=123.5,
        )
        bot = FakeBot(cached={thread.id: thread})
        cog = Counting(bot)

        _run(cog.on_ready())

        state = db.counting_get_channel(guild.id, thread.id)
        assert (state.current_count, state.last_message_id) == (3, '3')
        assert (state.configured_by, state.configured_at) == ('321', 123.5)
        assert [row.content for row in db.counting_get_attempts(
            guild.id, thread.id)] == ['1', '10', '3']
        assert len(thread.history_calls) == 1
        call = thread.history_calls[0]
        assert call['limit'] is None
        assert call['oldest_first'] is True
        assert call['before'] is None
        assert bot.fetch_calls == []
        assert all(message.reactions == [] for message in thread.messages)

        thread.messages.append(_message(4, '4', thread, guild, offset=4))
        _run(cog.on_ready())

        assert db.counting_get_channel(
            guild.id, thread.id).current_count == 3
        assert len(thread.history_calls) == 1

    def test_cache_miss_fetches_archived_thread(self, db):
        guild = FakeGuild()
        thread = _thread(201, guild, ['1'])
        db.counting_configure(
            guild.id, thread.id, current_count=9,
            configured_by=444, now=50)
        bot = FakeBot(fetched={thread.id: thread})

        _run(Counting(bot).on_ready())

        assert bot.fetch_calls == [thread.id]
        state = db.counting_get_channel(guild.id, thread.id)
        assert (state.current_count, state.configured_by,
                state.configured_at) == (1, '444', 50.0)

    def test_unavailable_and_failed_channels_do_not_stop_later_reparse(
            self, db, caplog):
        guild = FakeGuild()
        failing = FailingHistoryChannel(channel_id=201)
        failing.guild = guild
        good = _thread(202, guild, ['1'])
        for channel_id in (200, 201, 202):
            db.counting_configure(
                guild.id, channel_id, current_count=8,
                configured_by=99, now=10)
        bot = FakeBot(fetched={200: None, 201: failing, 202: good})

        with caplog.at_level(logging.WARNING):
            _run(Counting(bot).on_ready())

        assert db.counting_get_channel(guild.id, 200).current_count == 8
        assert db.counting_get_channel(guild.id, 201).current_count == 8
        assert db.counting_get_channel(guild.id, 202).current_count == 1
        assert bot.fetch_calls == [200, 201, 202]
        assert 'Counting channel unavailable at startup' in caplog.text
        assert 'Could not reparse counting history at startup' in caplog.text

    def test_wrong_guild_is_rejected_without_mutating_state(self, db, caplog):
        configured_guild = FakeGuild(100)
        other_guild = FakeGuild(999)
        thread = _thread(200, other_guild, ['1'])
        db.counting_configure(
            configured_guild.id, thread.id, current_count=8, now=10)

        with caplog.at_level(logging.WARNING):
            _run(Counting(FakeBot(cached={thread.id: thread})).on_ready())

        assert db.counting_get_channel(
            configured_guild.id, thread.id).current_count == 8
        assert 'Counting channel guild mismatch at startup' in caplog.text

    def test_waits_for_database_initialization(self, db, monkeypatch):
        guild = FakeGuild()
        thread = _thread(200, guild, ['1'])
        db.counting_configure(guild.id, thread.id, current_count=8, now=10)
        bot = FakeBot(cached={thread.id: thread})
        cog = Counting(bot)
        monkeypatch.setattr(cf_common, 'user_db', None)

        async def exercise():
            task = asyncio.create_task(cog.on_ready())
            await asyncio.sleep(0)
            assert not task.done()
            assert thread.history_calls == []
            cf_common.user_db = db
            await asyncio.wait_for(task, timeout=1)

        _run(exercise())

        assert db.counting_get_channel(guild.id, thread.id).current_count == 1

    def test_uses_channel_lock_while_reparsing(self, db):
        guild = FakeGuild()
        thread = _thread(200, guild, ['1'])
        db.counting_configure(guild.id, thread.id, current_count=8, now=10)
        cog = Counting(FakeBot(cached={thread.id: thread}))

        async def exercise():
            lock = cog._lock_for(guild.id, thread.id)
            await lock.acquire()
            startup = asyncio.create_task(cog.on_ready())
            await asyncio.sleep(0)
            assert not startup.done()
            assert thread.history_calls == []
            lock.release()
            await startup

        _run(exercise())

        assert db.counting_get_channel(guild.id, thread.id).current_count == 1
