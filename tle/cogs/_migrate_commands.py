"""Command implementation bodies for the starboard migration cog.

The ``@migrate.command`` callbacks must live in the cog class body (so the
command group/subcommand registration works), but their logic lives here as
``_impl_*`` mixin methods to keep ``migrate.py`` small.

Tunables (``_pause_kvs_key``) are read from ``tle.cogs.migrate`` at call time.
"""
import asyncio
import json
import logging
import time

import discord

from tle.util import codeforces_common as cf_common

logger = logging.getLogger(__name__)


def _paginate(header, lines, limit=1900):
    """Split header + lines into chunks under Discord's message limit."""
    chunks = []
    current = header
    for line in lines:
        if len(current) + len(line) + 1 > limit:
            chunks.append(current)
            current = line
        else:
            current += '\n' + line
    if current:
        chunks.append(current)
    return chunks


def _pause_kvs_key(guild_id):
    return f'migration_pre_pause_status:{guild_id}'


class MigrateCommandsMixin:
    """Implementation bodies for the migrate subcommands."""

    async def _impl_start(self, ctx, old_channel, new_channel, emojis):
        guild_id = ctx.guild.id

        if not emojis:
            await ctx.send('Please specify at least one emoji to migrate.')
            return

        existing = cf_common.user_db.get_migration(guild_id)
        if existing is not None:
            await ctx.send(f'A migration is already in progress (status: {existing.status}). '
                           f'Use `;migrate cancel` first.')
            return

        main_emoji = emojis[0]
        alias_emojis = list(emojis[1:])
        alias_map = {alias: main_emoji for alias in alias_emojis}

        emoji_csv = ','.join(emojis)
        cf_common.user_db.create_migration(
            guild_id, old_channel.id, new_channel.id, emoji_csv, time.time()
        )

        if alias_map:
            cf_common.user_db.set_migration_alias_map(guild_id, json.dumps(alias_map))

        emoji_set = set(emojis)
        task = asyncio.create_task(
            self._run_migration(guild_id, old_channel.id, new_channel.id, emoji_set)
        )
        self._tasks[guild_id] = task

        logger.info(f'Migration: guild={guild_id} started by {ctx.author} '
                     f'old={old_channel.id} new={new_channel.id} emojis={emoji_csv} '
                     f'aliases={alias_map}')

        desc = f'Migration started! Crawling {old_channel.mention} for {", ".join(emojis)}.'
        if alias_map:
            alias_desc = ', '.join(f'{a} → {m}' for a, m in alias_map.items())
            desc += f'\nAliases: {alias_desc}'
        desc += '\nUse `;migrate status` to check progress.'
        await ctx.send(desc)

    async def _impl_status(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration in progress.')
            return

        status_counts = cf_common.user_db.count_migration_entries_by_status(guild_id)
        counts = {r.crawl_status: r.cnt for r in status_counts}

        alias_map = cf_common.user_db.get_migration_alias_map(guild_id)

        lines = [
            f'**Migration Status:** {migration.status}',
            f'**Emojis:** {migration.emojis}',
            f'**Crawl:** {migration.crawl_done} done, {migration.crawl_failed} failed'
            f' (total: {migration.crawl_total})',
        ]

        if migration.status in ('posting', 'done', 'failed'):
            lines.append(f'**Post:** {migration.post_done}/{migration.post_total}')

        if alias_map:
            alias_desc = ', '.join(f'{a} → {m}' for a, m in alias_map.items())
            lines.append(f'**Aliases:** {alias_desc}')

        if counts:
            parts = [f'{k}: {v}' for k, v in sorted(counts.items())]
            lines.append(f'**Entries by status:** {", ".join(parts)}')

        await ctx.send('\n'.join(lines))

    async def _impl_complete(self, ctx, new_channel):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration to complete.')
            return

        if migration.status != 'done':
            await ctx.send(f'Migration is not done yet (status: {migration.status}). '
                           f'Wait for it to finish first.')
            return

        db = cf_common.user_db
        emojis = migration.emojis.split(',')

        posted_entries = db.get_posted_migration_entries(guild_id)

        # Warn about post_failed entries before completing
        failed_counts = db.count_migration_entries_by_status(guild_id)
        failed_map = {r.crawl_status: r.cnt for r in failed_counts}
        pf_count = failed_map.get('post_failed', 0)
        re_count = failed_map.get('retry_exhausted', 0)
        total_failed = pf_count + re_count

        if total_failed > 0:
            await ctx.send(f'**Warning:** {total_failed} entries failed and will not be '
                           f'imported. Use `;migrate retry-failed` to retry them, or proceed '
                           f'with `;migrate complete {new_channel.mention}` again to accept the loss.')
            if not posted_entries:
                await ctx.send('No posted entries to import. Use `;migrate retry-failed` '
                               'to retry failed entries first.')
                return

        logger.info(f'Migration complete: guild={guild_id} importing {len(posted_entries)} entries '
                     f'({pf_count} post_failed entries discarded)')

        # Load alias map
        alias_map = db.get_migration_alias_map(guild_id)
        main_emojis = set(emojis) - set(alias_map.keys()) if alias_map else set(emojis)

        imported = await self._complete_import(
            guild_id, db, posted_entries, emojis, alias_map, new_channel.id)

        emoji_list = ', '.join(main_emojis)
        alias_list = ', '.join(f'{a} → {m}' for a, m in alias_map.items())
        logger.info(f'Migration complete: guild={guild_id} done — '
                     f'{imported} imported, emojis={emoji_list}, aliases={alias_list}')
        msg = f'Migration complete! {imported} messages imported.\n'
        msg += f'Emoji config created for {emoji_list} in {new_channel.mention}.'
        if alias_map:
            msg += f'\nAliases registered: {alias_list}'
        msg += '\nLive reaction tracking is now active.'
        await ctx.send(msg)

    async def _impl_resume(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration to resume.')
            return

        if migration.status not in ('failed', 'crawling', 'posting'):
            await ctx.send(f'Migration cannot be resumed (status: {migration.status}). '
                           f'Use `;migrate complete` if status is done.')
            return

        if self._task_running(guild_id):
            await ctx.send('Migration task is already running.')
            return

        db = cf_common.user_db

        # Reset failed entries so they can be retried
        db.reset_post_failed_entries(guild_id)
        db.reset_retry_exhausted_entries(guild_id)

        # Determine which phase to resume.
        # crawl_total is set at the END of the crawl phase. If it's 0, the crawl
        # never finished and we must resume crawling — even if some entries are
        # already crawled (they were crawled before the crash).
        # If status is already 'posting', the crawl completed and we resume posting.
        if migration.status == 'posting' or (migration.status == 'failed' and migration.crawl_total > 0):
            db.update_migration_status(guild_id, 'posting')
        else:
            db.update_migration_status(guild_id, 'crawling')

        emoji_set = set(migration.emojis.split(','))
        self._launch(guild_id, migration, emoji_set)

        logger.info(f'Migration resume: guild={guild_id} by {ctx.author} '
                     f'(was status={migration.status})')
        await ctx.send(f'Migration resumed! Use `;migrate status` to check progress.')

    async def _impl_show_deleted(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration in progress.')
            return

        entries = cf_common.user_db.get_deleted_migration_entries(guild_id)

        if not entries:
            await ctx.send('No deleted/inaccessible messages found.')
            return

        header = f'**Deleted/Inaccessible Messages ({len(entries)})**\n'
        lines = []

        for i, entry in enumerate(entries, 1):
            old_link = (f'https://discord.com/channels/{guild_id}/'
                        f'{entry.old_channel_id}/{entry.old_bot_msg_id}')
            line = f'{i}. {entry.emoji} — [Old post]({old_link})'

            if entry.new_starboard_msg_id:
                new_link = (f'https://discord.com/channels/{guild_id}/'
                            f'{migration.new_channel_id}/{entry.new_starboard_msg_id}')
                line += f' | [New post]({new_link})'

            lines.append(line)

        for chunk in _paginate(header, lines):
            await ctx.send(chunk)

    async def _impl_retry_failed(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration in progress.')
            return

        if self._task_running(guild_id):
            await ctx.send('Migration task is already running.')
            return

        db = cf_common.user_db
        entries = db.get_retry_exhausted_entries(guild_id)

        if not entries:
            await ctx.send('No failed entries to retry.')
            return

        count = len(entries)
        db.reset_retry_exhausted_entries(guild_id)
        db.update_migration_status(guild_id, 'posting')

        emoji_set = set(migration.emojis.split(','))
        self._launch(guild_id, migration, emoji_set)

        logger.info(f'Migration retry-failed: guild={guild_id} by {ctx.author} '
                     f'retrying {count} entries')
        await ctx.send(f'Retrying {count} failed entries. Use `;migrate status` to check progress.')

    async def _impl_view_failed(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration in progress.')
            return

        entries = cf_common.user_db.get_retry_exhausted_entries(guild_id)

        if not entries:
            await ctx.send('No failed entries.')
            return

        header = f'**Failed Messages ({len(entries)})**\n'
        lines = []

        for i, entry in enumerate(entries, 1):
            old_link = (f'https://discord.com/channels/{guild_id}/'
                        f'{entry.old_channel_id}/{entry.old_bot_msg_id}')
            error = (entry.last_error or 'unknown')[:80]
            line = f'{i}. {entry.emoji} — [Old post]({old_link}) — `{error}`'
            lines.append(line)

        for chunk in _paginate(header, lines):
            await ctx.send(chunk)

    async def _impl_restart_post(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration in progress.')
            return

        # Stop any running/paused task first
        event = self._paused.pop(guild_id, None)
        if event is not None:
            event.set()
        cf_common.user_db.kvs_set(_pause_kvs_key(guild_id), '')
        task = self._tasks.pop(guild_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        db = cf_common.user_db
        new_channel = self.bot.get_channel(int(migration.new_channel_id))
        if new_channel is None:
            await ctx.send(f'New channel {migration.new_channel_id} not found.')
            return

        # Delete all messages the bot posted in the new channel
        msg_ids = db.get_all_posted_msg_ids(guild_id)
        await ctx.send(f'Deleting {len(msg_ids)} posted messages from {new_channel.mention}...')

        deleted, failed = await self._delete_posted_messages(new_channel, msg_ids)

        if failed > 0:
            await ctx.send(f'Warning: failed to delete {failed} messages '
                           f'(bot may lack Manage Messages permission).')

        # Reset all entries back to crawled/deleted
        db.reset_all_entries_for_repost(guild_id)
        db.update_migration_status(guild_id, 'posting')
        db.set_migration_post_totals(guild_id, 0)
        db.update_migration_post_done(guild_id, 0)

        # Re-launch post phase
        emoji_set = set(migration.emojis.split(','))
        self._launch(guild_id, migration, emoji_set)

        logger.info(f'Migration restart-post: guild={guild_id} by {ctx.author} '
                     f'deleted {deleted}/{len(msg_ids)} messages, re-posting')
        await ctx.send(f'Deleted {deleted} messages. Re-posting now. '
                       f'Use `;migrate status` to check progress.')

    async def _delete_posted_messages(self, new_channel, msg_ids):
        """Delete the given message ids from new_channel. Returns (deleted, failed)."""
        deleted = 0
        failed = 0
        for msg_id in msg_ids:
            try:
                msg = await new_channel.fetch_message(int(msg_id))
                await msg.delete()
                deleted += 1
            except discord.NotFound:
                deleted += 1  # already gone
            except (discord.Forbidden, discord.HTTPException) as e:
                failed += 1
                if failed <= 3:
                    logger.warning(f'Migration restart-post: failed to delete msg {msg_id}: {e}')
            await asyncio.sleep(0.3)
        return deleted, failed

    async def _impl_pause(self, ctx):
        guild_id = ctx.guild.id

        migration = cf_common.user_db.get_migration(guild_id)
        if migration is None:
            await ctx.send('No migration in progress.')
            return

        if migration.status == 'paused':
            await ctx.send('Migration is already paused.')
            return

        # Store the current status in KVS so unpause can restore it (survives restart)
        cf_common.user_db.kvs_set(_pause_kvs_key(guild_id), migration.status)
        cf_common.user_db.update_migration_status(guild_id, 'paused')

        # If a task is running, block it with an event
        if self._task_running(guild_id):
            event = asyncio.Event()
            self._paused[guild_id] = event

        logger.info(f'Migration pause: guild={guild_id} by {ctx.author} '
                     f'(was {migration.status})')
        await ctx.send('Migration paused. Use `;migrate unpause` to continue. '
                       'Safe to restart the server — it will NOT auto-resume.')

    async def _impl_unpause(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None or migration.status != 'paused':
            await ctx.send('Migration is not paused.')
            return

        # Restore the previous status from KVS
        kvs_key = _pause_kvs_key(guild_id)
        prev_status = cf_common.user_db.kvs_get(kvs_key) or 'crawling'
        cf_common.user_db.update_migration_status(guild_id, prev_status)
        cf_common.user_db.kvs_set(kvs_key, '')  # clean up

        # Unblock the in-memory event if a task is waiting
        event = self._paused.pop(guild_id, None)
        if event is not None:
            event.set()
            logger.info(f'Migration unpause: guild={guild_id} by {ctx.author} '
                         f'(restored to {prev_status}, task still running)')
            await ctx.send('Migration unpaused.')
        else:
            # No running task (server was restarted while paused) — re-launch
            emoji_set = set(migration.emojis.split(','))
            self._launch(guild_id, migration, emoji_set)
            logger.info(f'Migration unpause: guild={guild_id} by {ctx.author} '
                         f'(restored to {prev_status}, re-launched task)')
            await ctx.send('Migration unpaused and re-launched.')

    async def _impl_cancel(self, ctx):
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration to cancel.')
            return

        logger.info(f'Migration cancel: guild={guild_id} by {ctx.author} '
                     f'(was status={migration.status})')

        # Clean up pause state
        event = self._paused.pop(guild_id, None)
        if event is not None:
            event.set()
        cf_common.user_db.kvs_set(_pause_kvs_key(guild_id), '')

        # Cancel background task if running
        task = self._tasks.pop(guild_id, None)
        if task and not task.done():
            task.cancel()

        # Clean up DB
        cf_common.user_db.delete_migration_entries(guild_id)
        cf_common.user_db.delete_migration(guild_id)

        await ctx.send('Migration cancelled and data cleaned up.')

    async def _impl_resume_on_ready(self):
        """Resume any in-progress migrations after bot restart."""
        if cf_common.user_db is None:
            logger.debug('Migration: user_db not ready in on_ready, skipping resume')
            return

        resumed = 0
        for guild in self.bot.guilds:
            migration = cf_common.user_db.get_migration(guild.id)
            if migration is None:
                continue

            if migration.status in ('crawling', 'posting'):
                emoji_set = set(migration.emojis.split(','))
                logger.info(f'Migration resume: guild={guild.id} status={migration.status} '
                            f'checkpoint={migration.last_crawled_msg_id} '
                            f'crawl_done={migration.crawl_done} emojis={migration.emojis}')
                self._launch(guild.id, migration, emoji_set)
                resumed += 1

        if resumed:
            logger.info(f'Migration resume: {resumed} migration(s) resumed across all guilds')
