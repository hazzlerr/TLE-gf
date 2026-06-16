"""Pillboard export logic for the starboard migration cog.

Mixin methods used by ``tle.cogs.migrate.Migrate``. Reads tunables
(``_MAX_RETRIES``, ``_RETRY_BASE_DELAY``, ``_EXPORT_DIR``, etc.) from the
``tle.cogs.migrate`` module at call time so tests can monkeypatch them.
"""
import asyncio
import gzip
import io
import json
import logging
import time

import discord
from discord.ext import commands

from tle.cogs._starboard_helpers import _emoji_str
from tle.cogs._migrate_helpers import parse_old_bot_message
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError

logger = logging.getLogger(__name__)


def _cfg():
    from tle.cogs import migrate
    return migrate


class MigrateExportMixin:
    """Pillboard export to JSON."""

    @staticmethod
    def _message_iso(value):
        return value.isoformat() if value is not None else None

    @staticmethod
    def _message_user_payload(user):
        if user is None:
            return None
        return {
            'id': str(user.id),
            'name': getattr(user, 'name', None),
            'display_name': getattr(user, 'display_name', None),
            'bot': bool(getattr(user, 'bot', False)),
        }

    @staticmethod
    def _message_attachment_payload(attachment):
        return {
            'id': str(getattr(attachment, 'id', '')),
            'filename': getattr(attachment, 'filename', None),
            'url': getattr(attachment, 'url', None),
            'proxy_url': getattr(attachment, 'proxy_url', None),
            'content_type': getattr(attachment, 'content_type', None),
            'size': getattr(attachment, 'size', None),
            'width': getattr(attachment, 'width', None),
            'height': getattr(attachment, 'height', None),
            'spoiler': (
                attachment.is_spoiler()
                if hasattr(attachment, 'is_spoiler') else False
            ),
            'description': getattr(attachment, 'description', None),
        }

    @staticmethod
    def _message_embed_payload(embed):
        if hasattr(embed, 'to_dict'):
            return embed.to_dict()
        return dict(embed) if isinstance(embed, dict) else {'repr': repr(embed)}

    @staticmethod
    def _message_reaction_payload(reaction):
        return {
            'emoji': _emoji_str(reaction.emoji),
            'count': int(reaction.count),
        }

    def _message_json_payload(self, message):
        reference = None
        if getattr(message, 'reference', None) is not None:
            reference = {
                'message_id': (
                    str(message.reference.message_id)
                    if getattr(message.reference, 'message_id', None) else None
                ),
                'channel_id': (
                    str(message.reference.channel_id)
                    if getattr(message.reference, 'channel_id', None) else None
                ),
                'guild_id': (
                    str(message.reference.guild_id)
                    if getattr(message.reference, 'guild_id', None) else None
                ),
            }

        guild = getattr(message, 'guild', None)
        channel = getattr(message, 'channel', None)
        return {
            'id': str(message.id),
            'channel_id': str(channel.id) if channel is not None else None,
            'guild_id': str(guild.id) if guild is not None else None,
            'author': self._message_user_payload(
                getattr(message, 'author', None)),
            'created_at': self._message_iso(
                getattr(message, 'created_at', None)),
            'edited_at': self._message_iso(
                getattr(message, 'edited_at', None)),
            'jump_url': getattr(message, 'jump_url', None),
            'type': str(getattr(message, 'type', '')),
            'content': getattr(message, 'content', ''),
            'attachments': [
                self._message_attachment_payload(attachment)
                for attachment in getattr(message, 'attachments', [])
            ],
            'embeds': [
                self._message_embed_payload(embed)
                for embed in getattr(message, 'embeds', [])
            ],
            'reactions': [
                self._message_reaction_payload(reaction)
                for reaction in getattr(message, 'reactions', [])
            ],
            'reference': reference,
            'pinned': bool(getattr(message, 'pinned', False)),
            'tts': bool(getattr(message, 'tts', False)),
        }

    @staticmethod
    def _parse_export_args(args):
        cfg = _cfg()
        emojis = []
        limit = None
        context_limit = 0
        for arg in args:
            if arg.startswith('+limit='):
                try:
                    limit = int(arg.split('=', 1)[1])
                except ValueError as exc:
                    raise commands.BadArgument(
                        '+limit must be an integer.') from exc
                if limit <= 0:
                    raise commands.BadArgument(
                        '+limit must be a positive integer.')
            elif arg.startswith('+context='):
                try:
                    context_limit = int(arg.split('=', 1)[1])
                except ValueError as exc:
                    raise commands.BadArgument(
                        '+context must be an integer.') from exc
                if context_limit < 0:
                    raise commands.BadArgument(
                        '+context must be a non-negative integer.')
                if context_limit > cfg._EXPORT_CONTEXT_LIMIT_MAX:
                    raise commands.BadArgument(
                        f'+context must be at most {cfg._EXPORT_CONTEXT_LIMIT_MAX}.')
            else:
                emojis.append(arg)
        return set(emojis), limit, context_limit

    async def _fetch_message_context_before(self, source_channel, original_msg,
                                            context_limit):
        if context_limit <= 0:
            return []
        cfg = _cfg()

        async def fetch_context():
            messages = []
            async for msg in source_channel.history(
                    before=original_msg, limit=context_limit,
                    oldest_first=False):
                messages.append(msg)
            messages.reverse()
            return [self._message_json_payload(msg) for msg in messages]

        return await discord_retry(
            fetch_context,
            max_retries=cfg._MAX_RETRIES,
            base_delay=cfg._RETRY_BASE_DELAY,
        )

    async def _export_one_row(self, old_bot_msg, parsed, context_limit):
        """Build a single export row (and fetch original + context). Returns
        (row, fetched, failed, context_fetched, context_failed)."""
        cfg = _cfg()
        (emoji, displayed_count, guild_id, source_channel_id,
         original_msg_id) = parsed
        fetched = failed = context_fetched = context_failed = 0

        row = {
            'pillboard': {
                'message': self._message_json_payload(old_bot_msg),
                'emoji': emoji,
                'displayed_count': displayed_count,
            },
            'original_ref': {
                'guild_id': str(guild_id),
                'channel_id': str(source_channel_id),
                'message_id': str(original_msg_id),
                'jump_url': (
                    f'https://discord.com/channels/{guild_id}/'
                    f'{source_channel_id}/{original_msg_id}'
                ),
            },
            'original': None,
            'context_before': [],
            'context_fetch_status': (
                'not_requested' if context_limit == 0 else 'pending'
            ),
            'context_fetch_error': None,
            'fetch_status': 'ok',
            'fetch_error': None,
        }
        try:
            source_channel = await self._fetch_source_channel(source_channel_id)
            original_msg = await discord_retry(
                lambda: source_channel.fetch_message(original_msg_id),
                max_retries=cfg._MAX_RETRIES,
                base_delay=cfg._RETRY_BASE_DELAY,
            )
        except (discord.NotFound, discord.Forbidden,
                discord.HTTPException, RetryExhaustedError) as exc:
            row['fetch_status'] = type(exc).__name__
            row['fetch_error'] = str(exc)
            if context_limit > 0:
                row['context_fetch_status'] = 'original_fetch_failed'
            failed += 1
        else:
            row['original'] = self._message_json_payload(original_msg)
            fetched += 1
            if context_limit > 0:
                try:
                    row['context_before'] = (
                        await self._fetch_message_context_before(
                            source_channel, original_msg, context_limit)
                    )
                except (discord.NotFound, discord.Forbidden,
                        discord.HTTPException,
                        RetryExhaustedError) as exc:
                    row['context_fetch_status'] = type(exc).__name__
                    row['context_fetch_error'] = str(exc)
                    context_failed += 1
                else:
                    row['context_fetch_status'] = 'ok'
                    context_fetched += 1
        return row, fetched, failed, context_fetched, context_failed

    async def _build_pillboard_export(self, old_channel, emoji_filter, limit,
                                      context_limit=0, progress_cb=None):
        cfg = _cfg()
        rows = []
        scanned = 0
        parsed_count = 0
        fetched = 0
        failed = 0
        context_fetched = 0
        context_failed = 0

        async for old_bot_msg in old_channel.history(
                oldest_first=True, limit=limit):
            scanned += 1
            parsed = parse_old_bot_message(old_bot_msg.content or '')
            if parsed is None:
                continue
            emoji = parsed[0]
            if emoji_filter and emoji not in emoji_filter:
                continue
            parsed_count += 1

            (row, f, fa, cf, cfa) = await self._export_one_row(
                old_bot_msg, parsed, context_limit)
            fetched += f
            failed += fa
            context_fetched += cf
            context_failed += cfa
            rows.append(row)
            if (
                    progress_cb is not None
                    and parsed_count % cfg._EXPORT_PROGRESS_INTERVAL == 0):
                await progress_cb({
                    'scanned': scanned,
                    'parsed': parsed_count,
                    'fetched': fetched,
                    'failed': failed,
                })
            await asyncio.sleep(0)

        return {
            'scanned': scanned,
            'parsed': parsed_count,
            'fetched': fetched,
            'failed': failed,
            'context_fetched': context_fetched,
            'context_failed': context_failed,
            'rows': rows,
        }

    async def _cmd_pillboard_export(self, ctx, old_channel, *args):
        cfg = _cfg()
        try:
            emoji_filter, limit, context_limit = self._parse_export_args(args)
        except commands.BadArgument as exc:
            await ctx.send(str(exc))
            return

        progress_message = await ctx.send(
            f'Exporting pillboard messages from {old_channel.mention}. '
            'This might take a while. Progress updates every '
            f'{cfg._EXPORT_PROGRESS_INTERVAL} parsed post(s).')
        started_at = time.time()

        async def progress_cb(progress):
            if progress_message is None or not hasattr(progress_message, 'edit'):
                return
            elapsed = int(time.time() - started_at)
            try:
                await progress_message.edit(content=(
                    f'Exporting pillboard messages from {old_channel.mention}: '
                    f'scanned **{progress["scanned"]}**, '
                    f'parsed **{progress["parsed"]}**, '
                    f'fetched **{progress["fetched"]}**, '
                    f'failed **{progress["failed"]}** '
                    f'({elapsed}s).'
                ))
            except discord.HTTPException:
                pass

        result = await self._build_pillboard_export(
            old_channel, emoji_filter, limit, context_limit,
            progress_cb=progress_cb)
        payload = {
            'exported_at': time.time(),
            'guild_id': str(ctx.guild.id),
            'pillboard_channel_id': str(old_channel.id),
            'emoji_filter': sorted(emoji_filter),
            'limit': limit,
            'context_limit': context_limit,
            'summary': {
                'scanned': result['scanned'],
                'parsed': result['parsed'],
                'fetched': result['fetched'],
                'failed': result['failed'],
                'context_fetched': result['context_fetched'],
                'context_failed': result['context_failed'],
                'seconds': round(time.time() - started_at, 3),
            },
            'messages': result['rows'],
        }
        data = json.dumps(payload, indent=2, ensure_ascii=False).encode('utf-8')

        cfg._EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        filename = (
            f'pillboard_export_{ctx.guild.id}_{old_channel.id}_'
            f'{int(started_at)}.json'
        )
        path = cfg._EXPORT_DIR / filename
        path.write_bytes(data)

        summary = (
            f'Pillboard export complete: scanned **{result["scanned"]}**, '
            f'parsed **{result["parsed"]}**, fetched **{result["fetched"]}**, '
            f'failed **{result["failed"]}**'
        )
        if context_limit > 0:
            summary += (
                f', context fetched **{result["context_fetched"]}**, '
                f'context failed **{result["context_failed"]}**'
            )
        summary += '.'
        upload_limit = getattr(ctx.guild, 'filesize_limit', 8 * 1024 * 1024)
        if len(data) <= upload_limit:
            await ctx.send(
                summary,
                file=discord.File(io.BytesIO(data), filename=filename))
            return

        gz_data = gzip.compress(data)
        gz_filename = f'{filename}.gz'
        gz_path = cfg._EXPORT_DIR / gz_filename
        gz_path.write_bytes(gz_data)
        if len(gz_data) <= upload_limit:
            await ctx.send(
                f'{summary}\nRaw JSON was too large for Discord, so I attached '
                'a compressed `.json.gz` file.',
                file=discord.File(io.BytesIO(gz_data), filename=gz_filename))
        else:
            await ctx.send(
                f'{summary}\nJSON is too large to upload, even compressed '
                f'({len(gz_data)} compressed bytes; limit {upload_limit}). '
                f'Saved on the server at `{path}` and `{gz_path}`.')
