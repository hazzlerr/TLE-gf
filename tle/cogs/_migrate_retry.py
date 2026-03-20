"""Exponential backoff retry helper for Discord API calls during migration."""
import asyncio
import logging
import random

import discord

logger = logging.getLogger(__name__)

# Exceptions that are permanent — never retry these
_PERMANENT = (discord.NotFound, discord.Forbidden)


class RetryExhaustedError(Exception):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, last_exception):
        self.last_exception = last_exception
        super().__init__(f'Retry exhausted: {last_exception}')


async def discord_retry(coro_factory, *, max_retries=5, base_delay=2.0, max_delay=60.0):
    """Call *coro_factory()* with exponential backoff on transient Discord errors.

    coro_factory: zero-arg callable returning a fresh coroutine each call.
    Retries on discord.HTTPException / discord.DiscordServerError.
    Does NOT retry discord.NotFound or discord.Forbidden (permanent).

    Raises RetryExhaustedError after max_retries failed attempts.
    """
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return await coro_factory()
        except _PERMANENT:
            raise  # propagate immediately
        except discord.HTTPException as exc:
            last_exc = exc
            if attempt == max_retries:
                break
            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter = delay * random.uniform(0, 0.25)
            sleep_time = delay + jitter
            logger.warning(f'discord_retry: attempt {attempt + 1}/{max_retries + 1} failed '
                           f'({exc}), retrying in {sleep_time:.1f}s')
            await asyncio.sleep(sleep_time)

    raise RetryExhaustedError(last_exc)
