"""Crawl + post phase logic for the starboard migration cog.

These are mixin methods used by ``tle.cogs.migrate.Migrate``. They are split
out of the cog body to keep every module under the line limit. The mixin reads
tunables (``_RATE_DELAY``, ``_MAX_RETRIES``, ``_RETRY_BASE_DELAY``) from the
``tle.cogs.migrate`` module at call time so tests can monkeypatch them there.
"""
import asyncio
import logging

import discord

from tle.util import codeforces_common as cf_common
from tle.cogs._starboard_helpers import _emoji_str
from tle.cogs._migrate_helpers import (
    parse_old_bot_message,
    serialize_embed_fallback,
    build_fallback_message,
)
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError

logger = logging.getLogger(__name__)


def _cfg():
    """Return the migrate module so tunables resolve to live (patchable) values."""
    from tle.cogs import migrate
    return migrate


class MigratePhasesMixin:
    """Crawl + post background phases."""

    async def _wait_if_paused(self, guild_id):
        """Block until unpaused. Returns immediately if not paused."""
        event = self._paused.get(guild_id)
        if event is not None and not event.is_set():
            logger.info(f'Migration: guild={guild_id} paused, waiting...')
            await event.wait()
            logger.info(f'Migration: guild={guild_id} resumed from pause')

    async def _run_migration(self, guild_id, old_channel_id, new_channel_id, emoji_set):
        """Background task: crawl old channel, then post to new channel."""
        await self.bot.wait_until_ready()
        db = cf_common.user_db

        logger.info(f'=== MIGRATION START === guild={guild_id}')

        try:
            migration = db.get_migration(guild_id)
            if migration is None:
                logger.warning(f'Migration: guild={guild_id} migration record not found, aborting')
                return

            # Skip crawl if already in posting phase (resume from posting)
            if migration.status != 'posting':
                await self._crawl_phase(guild_id, old_channel_id, emoji_set, db)

                migration = db.get_migration(guild_id)
                if migration is None or migration.status == 'failed':
                    logger.warning(f'Migration: guild={guild_id} crawl phase ended with '
                                   f'status={migration.status if migration else "deleted"}')
                    return

                db.update_migration_status(guild_id, 'posting')

            await self._post_phase(guild_id, new_channel_id, emoji_set, db)

            db.update_migration_status(guild_id, 'done')
            logger.info(f'=== MIGRATION COMPLETE === guild={guild_id}')

        except asyncio.CancelledError:
            logger.info(f'Migration: guild={guild_id} cancelled')
            # Don't update status — the cancel command already cleaned up
            raise
        except Exception as e:
            logger.error(f'Migration: guild={guild_id} FAILED: {e}', exc_info=True)
            db.update_migration_status(guild_id, 'failed')
        finally:
            self._tasks.pop(guild_id, None)

    async def _fetch_source_channel(self, source_channel_id):
        """Get a source channel, falling back to fetch_channel for threads."""
        ch = self.bot.get_channel(source_channel_id)
        if ch is not None:
            return ch
        cfg = _cfg()
        return await discord_retry(
            lambda: self.bot.fetch_channel(source_channel_id),
            max_retries=cfg._MAX_RETRIES, base_delay=cfg._RETRY_BASE_DELAY,
        )

    async def _crawl_phase(self, guild_id, old_channel_id, emoji_set, db):
        """Crawl the old bot's channel, collecting entries and reactors."""
        old_channel = self.bot.get_channel(old_channel_id)
        if old_channel is None:
            logger.error(f'Migration: guild={guild_id} old channel {old_channel_id} not found')
            db.update_migration_status(guild_id, 'failed')
            return

        migration = db.get_migration(guild_id)
        after = None
        if migration.last_crawled_msg_id:
            after = discord.Object(id=int(migration.last_crawled_msg_id))

        crawl_done = migration.crawl_done
        crawl_failed = migration.crawl_failed

        logger.info(f'Migration crawl: guild={guild_id} channel={old_channel_id} '
                     f'checkpoint={migration.last_crawled_msg_id} '
                     f'done={crawl_done} failed={crawl_failed}')

        async for old_bot_msg in old_channel.history(after=after, oldest_first=True, limit=None):
            if not old_bot_msg.content:
                continue

            parsed = parse_old_bot_message(old_bot_msg.content)
            if parsed is None:
                continue

            crawl_done, crawl_failed = await self._crawl_one(
                guild_id, old_channel_id, old_bot_msg, parsed, emoji_set, db,
                crawl_done, crawl_failed)

            # Checkpoint after each message
            db.update_migration_checkpoint(
                guild_id, str(old_bot_msg.id), crawl_done, crawl_failed
            )

            await self._wait_if_paused(guild_id)
            await asyncio.sleep(_cfg()._RATE_DELAY)

        db.set_migration_crawl_total(guild_id, crawl_done)
        logger.info(f'Migration crawl: guild={guild_id} finished — '
                     f'{crawl_done} processed, {crawl_failed} failed')

    async def _crawl_one(self, guild_id, old_channel_id, old_bot_msg, parsed,
                         emoji_set, db, crawl_done, crawl_failed):
        """Process a single parsed old-bot message. Returns updated counters."""
        cfg = _cfg()
        (_parsed_emoji, displayed_count, _msg_guild_id,
         source_channel_id, original_msg_id) = parsed

        # Try to fetch the original message with retry
        original_msg = None
        try:
            source_channel = await self._fetch_source_channel(source_channel_id)
            original_msg = await discord_retry(
                lambda: source_channel.fetch_message(original_msg_id),
                max_retries=cfg._MAX_RETRIES, base_delay=cfg._RETRY_BASE_DELAY,
            )
        except (discord.NotFound, discord.Forbidden):
            pass  # permanent — message deleted or no access
        except RetryExhaustedError as e:
            db.add_migration_entry(
                guild_id, str(original_msg_id), _parsed_emoji,
                str(old_bot_msg.id), str(old_channel_id)
            )
            db.update_migration_entry_retry_exhausted(
                str(original_msg_id), _parsed_emoji, str(e.last_exception))
            crawl_done += 1
            crawl_failed += 1
            logger.warning(f'Migration crawl: guild={guild_id} [{crawl_done}] '
                           f'msg={original_msg_id} RETRY EXHAUSTED: {e}')
            return crawl_done, crawl_failed

        # Always save the old bot's message as fallback — even for crawled
        # entries, the original might be temporarily unavailable during posting
        fallback = serialize_embed_fallback(old_bot_msg)

        if original_msg is not None:
            await self._crawl_record_reactions(
                guild_id, old_channel_id, old_bot_msg, original_msg,
                _parsed_emoji, displayed_count, source_channel_id,
                emoji_set, fallback, db, crawl_done)
            crawl_done += 1
            logger.info(f'Migration crawl: guild={guild_id} [{crawl_done}] '
                        f'msg={original_msg_id} done')
        else:
            # Original message deleted, inaccessible, or channel gone.
            # Use the parsed emoji from the old bot's message header.
            emoji_str = _parsed_emoji
            fallback = serialize_embed_fallback(old_bot_msg)
            db.add_migration_entry(
                guild_id, str(original_msg_id), emoji_str,
                str(old_bot_msg.id), str(old_channel_id)
            )
            db.update_migration_entry_deleted(
                str(original_msg_id), emoji_str, fallback
            )
            crawl_done += 1
            crawl_failed += 1
            logger.info(f'Migration crawl: guild={guild_id} [{crawl_done}] '
                        f'emoji={emoji_str} msg={original_msg_id} DELETED/INACCESSIBLE')

        return crawl_done, crawl_failed

    async def _crawl_record_reactions(self, guild_id, old_channel_id, old_bot_msg,
                                      original_msg, _parsed_emoji, displayed_count,
                                      source_channel_id, emoji_set, fallback, db,
                                      crawl_done):
        """Scan ALL reactions on the original message for emoji we care about.

        Don't trust the old bot's display emoji — just look at the actual
        reactions.
        """
        original_msg_id = original_msg.id
        found_any = False
        for reaction in original_msg.reactions:
            emoji_str = _emoji_str(reaction.emoji)
            if emoji_str not in emoji_set:
                continue
            found_any = True

            # Add entry (idempotent for resume)
            db.add_migration_entry(
                guild_id, str(original_msg_id), emoji_str,
                str(old_bot_msg.id), str(old_channel_id)
            )

            star_count = reaction.count
            reactor_ids = []
            try:
                async for user in reaction.users():
                    reactor_ids.append(str(user.id))
            except discord.HTTPException as e:
                logger.warning(f'Migration crawl: guild={guild_id} [{crawl_done + 1}] '
                               f'reactor fetch failed for msg={original_msg_id} '
                               f'emoji={emoji_str}: {e}')

            if reactor_ids:
                db.bulk_add_reactors(str(original_msg_id), emoji_str, reactor_ids)

            db.update_migration_entry_crawled(
                str(original_msg_id), emoji_str,
                str(source_channel_id), str(original_msg.author.id),
                star_count, fallback
            )
            logger.info(f'Migration crawl: guild={guild_id} [{crawl_done + 1}] '
                        f'emoji={emoji_str} msg={original_msg_id} '
                        f'author={original_msg.author} count={star_count}')

        if not found_any:
            # Original exists but has no matching reactions — reactions
            # were likely on the old bot's starboard post, not the original.
            # Use the displayed count from the old bot's header and mark
            # as crawled (not deleted — the message is real).
            emoji_str = _parsed_emoji
            db.add_migration_entry(
                guild_id, str(original_msg_id), emoji_str,
                str(old_bot_msg.id), str(old_channel_id)
            )
            db.update_migration_entry_crawled(
                str(original_msg_id), emoji_str,
                str(source_channel_id), str(original_msg.author.id),
                displayed_count, fallback
            )

    async def _post_phase(self, guild_id, new_channel_id, emoji_set, db):
        """Post crawled entries to the new starboard channel in chronological order.

        Each entry is posted with its original emoji — no merging or conversion.
        Alias resolution only happens later during ;migrate complete.
        Uses exponential backoff on transient Discord errors.
        """
        new_channel = self.bot.get_channel(new_channel_id)
        if new_channel is None:
            logger.error(f'Migration post: guild={guild_id} new channel {new_channel_id} not found')
            db.update_migration_status(guild_id, 'failed')
            return

        entries = db.get_migration_entries_for_posting(guild_id)
        db.set_migration_post_totals(guild_id, len(entries))

        logger.info(f'Migration post: guild={guild_id} starting — {len(entries)} entries to post')

        post_done = 0
        post_failed = 0

        # Get the old channel for fetching old bot messages on the fly
        old_channel = self.bot.get_channel(int(
            db.get_migration(guild_id).old_channel_id))

        for entry in entries:
            post_done, post_failed = await self._post_one(
                guild_id, new_channel, old_channel, entry, db,
                post_done, post_failed, len(entries))

            await self._wait_if_paused(guild_id)
            await asyncio.sleep(_cfg()._RATE_DELAY)

        logger.info(f'Migration post: guild={guild_id} finished — '
                     f'{post_done}/{len(entries)} ({post_failed} failed)')

    async def _post_one(self, guild_id, new_channel, old_channel, entry, db,
                        post_done, post_failed, total):
        """Post a single entry. Returns updated (post_done, post_failed)."""
        cfg = _cfg()
        try:
            content, embeds = await self._build_post_message(
                old_channel, entry, db)

            sent = await discord_retry(
                lambda: new_channel.send(content=content, embeds=embeds),
                max_retries=cfg._MAX_RETRIES, base_delay=cfg._RETRY_BASE_DELAY,
            )

            db.update_migration_entry_posted(entry.original_msg_id, entry.emoji, str(sent.id))
            post_done += 1
            db.update_migration_post_done(guild_id, post_done)

            logger.info(f'Migration post: guild={guild_id} [{post_done}/{total}] '
                        f'msg={entry.original_msg_id} emoji={entry.emoji}')

        except RetryExhaustedError as e:
            logger.error(f'Migration post: guild={guild_id} RETRY EXHAUSTED '
                         f'msg={entry.original_msg_id} emoji={entry.emoji}: {e}')
            db.update_migration_entry_retry_exhausted(
                entry.original_msg_id, entry.emoji, str(e.last_exception))
            post_done += 1
            post_failed += 1
            db.update_migration_post_done(guild_id, post_done)
        except Exception as e:
            logger.error(f'Migration post: guild={guild_id} FAILED '
                         f'msg={entry.original_msg_id} emoji={entry.emoji}: {e}',
                         exc_info=True)
            db.update_migration_entry_retry_exhausted(
                entry.original_msg_id, entry.emoji, str(e))
            post_done += 1
            post_failed += 1
            db.update_migration_post_done(guild_id, post_done)

        return post_done, post_failed

    async def _build_post_message(self, old_channel, entry, db):
        """Build (content, embeds) for a post-phase entry.

        Uses stored embed_fallback if present, else fetches the old bot message
        on the fly (and caches it), else falls back to a basic content line.
        """
        cfg = _cfg()
        if entry.embed_fallback:
            return build_fallback_message(
                entry, entry.embed_fallback, entry.emoji)
        if old_channel is not None:
            try:
                old_bot_msg = await discord_retry(
                    lambda: old_channel.fetch_message(int(entry.old_bot_msg_id)),
                    max_retries=cfg._MAX_RETRIES, base_delay=cfg._RETRY_BASE_DELAY,
                )
                # Save it so we don't fetch again on retry
                fallback = serialize_embed_fallback(old_bot_msg)
                db.set_embed_fallback(entry.original_msg_id, entry.emoji, fallback)
                return build_fallback_message(entry, fallback, entry.emoji)
            except (discord.NotFound, discord.Forbidden, RetryExhaustedError):
                return build_fallback_message(entry, None, entry.emoji)
        return build_fallback_message(entry, None, entry.emoji)
