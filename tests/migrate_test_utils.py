"""Shared fakes for the (split) migration cog test modules.

Re-exports the fixtures/fakes from ``tests._migrate_fakes`` and adds the
``_FakeGuild`` / ``_FakeCtx`` helpers and the ``_zero_retry_delay`` fixture that
the command-level tests use. This is NOT a test file (no ``test_`` prefix), so
pytest won't collect it.
"""
import pytest

from tests._migrate_fakes import (  # noqa: F401
    _FakeUser,
    _FakeReaction,
    _FakeMessage,
    _FakeChannel,
    _FakeBot,
    _FakeMigrateDb,
    _run,
    GUILD,
    PILL,
    CHOC,
    db,
    _zero_rate_delay,
)


class _FakeGuild:
    def __init__(self, guild_id=GUILD):
        self.id = guild_id


class _FakeCtx:
    """Minimal ctx for testing command methods directly."""
    def __init__(self, guild_id=GUILD):
        self.guild = _FakeGuild(guild_id)
        self.author = _FakeUser()
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append(content)


@pytest.fixture(autouse=True)
def _zero_retry_delay(monkeypatch):
    """Patch retry base delay to 0 so tests don't sleep."""
    import tle.cogs.migrate as _mod
    monkeypatch.setattr(_mod, '_RETRY_BASE_DELAY', 0)
