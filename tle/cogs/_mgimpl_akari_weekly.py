"""Weekly Akari rating displays and scheduled result announcements."""

import asyncio
import datetime as dt
import logging
import re
import time
from zoneinfo import ZoneInfo

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks
from tle.util.akari_difficulty import fetch_akari_difficulties
from tle.util.akari_weekly import (
    compute_weekly_ratings, current_week_standings, week_start,
)
from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import MinigameCogError, _mg


logger = logging.getLogger(__name__)

_AKARI_DIFFICULTY_FETCH_TIMEOUT = 30
_AKARI_WEEKLY_POST_CHANNEL_KEY = 'akari_weekly_post_channel'
_AKARI_WEEKLY_POST_TIME_KEY = 'akari_weekly_post_time'
_AKARI_WEEKLY_POST_LAST_PREFIX = 'akari_weekly_post_last:'
_AKARI_WEEKLY_POST_CHECK_INTERVAL = 5 * 60
_AKARI_WEEKLY_POST_TZ = 'America/New_York'
_AKARI_WEEKLY_POST_DEFAULT_TIME = '00:00'


def _parse_akari_weekly_post_time(value):
    """Validate HH:MM and return a normalized value plus ``datetime.time``."""
    value = str(value)
    if re.fullmatch(r'\d{2}:\d{2}', value) is None:
        raise ValueError('Time must use 24-hour `HH:MM` format.')
    try:
        parsed = dt.datetime.strptime(value, '%H:%M').time()
    except (TypeError, ValueError) as exc:
        raise ValueError('Time must use 24-hour `HH:MM` format.') from exc
    return parsed.strftime('%H:%M'), parsed


def _akari_weekly_period_dates(now, post_time):
    """Return rating-as-of and current-standings dates for this cutoff."""
    _normalized, cutoff_time = _parse_akari_weekly_post_time(post_time)
    monday = week_start(now.date())
    cutoff = dt.datetime.combine(monday, cutoff_time, tzinfo=now.tzinfo)
    if now < cutoff:
        # Until the configured Monday cutoff, Sunday still belongs to the
        # in-progress weekly contest and must not appear in full ratings.
        return monday - dt.timedelta(days=1), monday - dt.timedelta(days=7)
    return monday, monday


class ImplAkariWeeklyMixin:
    @staticmethod
    def _akari_weekly_post_time(guild_id):
        value = cf_common.user_db.get_guild_config(
            guild_id, _AKARI_WEEKLY_POST_TIME_KEY)
        return value or _AKARI_WEEKLY_POST_DEFAULT_TIME

    def _akari_weekly_display_dates(self, guild_id):
        now = dt.datetime.now(ZoneInfo(_AKARI_WEEKLY_POST_TZ))
        return _akari_weekly_period_dates(
            now, self._akari_weekly_post_time(guild_id))

    async def _cmd_akari_weekly_ratings(
            self, ctx, *, excluded_ids=None, included_ids=None,
            include_inactive=False, show_all=False):
        """Render ratings produced by completed Monday-Sunday weeks."""
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        as_of_date, standings_date = self._akari_weekly_display_dates(
            ctx.guild.id)
        rows, _standings = await self._akari_weekly_snapshot(
            ctx.guild.id,
            excluded_ids=excluded_ids,
            included_ids=included_ids,
            as_of_date=as_of_date,
            standings_date=standings_date,
        )
        if not rows:
            raise MinigameCogError(
                f'No completed {AKARI_GAME.display_name} weekly ratings yet.')

        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        eligible = rows if show_all else [
            row for row in rows if row.user_id in registrants]
        shown = self._active_ranking_rows(
            eligible, include_inactive=include_inactive)

        if shown:
            qualifiers = []
            if show_all:
                qualifiers.append('all')
            if include_inactive:
                qualifiers.append('incl. inactive')
            suffix = f' ({", ".join(qualifiers)})' if qualifiers else ''
            title = f'Daily Akari Weekly Ratings{suffix}'
            rating_file = _mg()._get_akari_rating_table_image_file(
                ctx.guild, shown, registrants,
                title=title,
                mark_registered=show_all,
                games_label='Weeks',
            )
            await ctx.send(file=rating_file)
        else:
            raise MinigameCogError(
                f'No eligible {AKARI_GAME.display_name} weekly ratings found.')

    async def _cmd_akari_current_ratings(
            self, ctx, *, excluded_ids=None, included_ids=None,
            show_all=False):
        """Render only the provisional standings for the current week."""
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        as_of_date, standings_date = self._akari_weekly_display_dates(
            ctx.guild.id)
        _rows, standings = await self._akari_weekly_snapshot(
            ctx.guild.id,
            excluded_ids=excluded_ids,
            included_ids=included_ids,
            as_of_date=as_of_date,
            standings_date=standings_date,
        )
        if not show_all:
            registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
            standings = [
                row for row in standings if row.user_id in registrants
            ]
        await self._send_akari_current_week_scores(ctx, standings)

    @staticmethod
    async def _send_akari_current_week_scores(ctx, standings):
        if not standings:
            await ctx.send(embed=discord_common.embed_neutral(
                'No Daily Akari scores have been posted this week yet.'))
            return
        start = standings[0].week_start
        end = standings[0].week_end
        score_file = _mg()._get_akari_weekly_table_image_file(
            ctx.guild, standings,
            title=(f'Daily Akari Current Weekly Ratings · {start:%b %d}–'
                   f'{end:%b %d} (in progress)'))
        await ctx.send(file=score_file)

    async def _akari_weekly_snapshot(
            self, guild_id, *, excluded_ids=None, included_ids=None,
            as_of_date=None, standings_date=None):
        """Return completed-week ratings and one week's score standings."""
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids,
            included_ids=included_ids)
        today = as_of_date or dt.date.today()
        standings_date = standings_date or today
        current_puzzle = _mg().expected_puzzle_number(today)
        wanted = set()
        for row in result_rows:
            try:
                row_date = dt.date.fromisoformat(str(row.puzzle_date))
            except ValueError:
                continue
            monday_number = int(row.puzzle_number) - row_date.weekday()
            wanted.update(range(monday_number, monday_number + 7))
        monday_number = _mg().expected_puzzle_number(week_start(today))
        wanted.update(range(monday_number, current_puzzle + 1))
        difficulties = await self._akari_difficulty_map(wanted)
        states = compute_weekly_ratings(
            result_rows, difficulties, as_of_date=today)
        rating_rows = sorted(
            states.values(), key=lambda s: (-s.rating, -s.games, int(s.user_id)))
        standings = current_week_standings(
            result_rows, difficulties, as_of_date=standings_date)
        return rating_rows, standings

    # Keep the old helper name available for downstream callers introduced
    # with the original weekly-preview feature.
    _akari_weekly_preview = _akari_weekly_snapshot

    async def _cmd_akari_weekly_post_show(self, ctx):
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, _AKARI_WEEKLY_POST_CHANNEL_KEY)
        target = f'<#{channel_id}>' if channel_id else 'not set'
        post_time = self._akari_weekly_post_time(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_neutral(
            f'Weekly Akari result thread/channel: {target}\n'
            f'Weekly cutoff/post time: `{post_time}` '
            f'({_AKARI_WEEKLY_POST_TZ})'))

    async def _cmd_akari_weekly_post_here(self, ctx, channel=None):
        channel = channel or ctx.channel
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _AKARI_WEEKLY_POST_CHANNEL_KEY,
            str(channel.id))
        await ctx.send(embed=discord_common.embed_success(
            f'Weekly Akari results will be posted in <#{channel.id}> '
            'after each week ends.'))

    async def _cmd_akari_weekly_post_clear(self, ctx):
        cf_common.user_db.delete_guild_config(
            ctx.guild.id, _AKARI_WEEKLY_POST_CHANNEL_KEY)
        await ctx.send(embed=discord_common.embed_success(
            'Automatic weekly Akari result posting disabled.'))

    async def _cmd_akari_weekly_post_time(self, ctx, value):
        try:
            normalized, _parsed = _parse_akari_weekly_post_time(value)
        except ValueError as exc:
            raise MinigameCogError(str(exc)) from exc
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _AKARI_WEEKLY_POST_TIME_KEY, normalized)
        await ctx.send(embed=discord_common.embed_success(
            f'Weekly Akari cutoff and post time set to `{normalized}` '
            f'({_AKARI_WEEKLY_POST_TZ}).'))

    @tasks.task_spec(
        name='AkariWeeklyAnnouncementCheck',
        waiter=tasks.Waiter.fixed_delay(_AKARI_WEEKLY_POST_CHECK_INTERVAL),
    )
    async def _akari_weekly_announcement_check(self, _):
        if cf_common.user_db is None:
            return
        now = dt.datetime.now(ZoneInfo(_AKARI_WEEKLY_POST_TZ))
        for guild in self.bot.guilds:
            try:
                _as_of_date, current_start = _akari_weekly_period_dates(
                    now, self._akari_weekly_post_time(guild.id))
                completed_start = current_start - dt.timedelta(days=7)
                await self._check_akari_weekly_announcement_guild(
                    guild, completed_start)
            except Exception:
                logger.warning(
                    'Weekly Akari announcement check failed for guild=%s',
                    getattr(guild, 'id', None), exc_info=True)

    async def _check_akari_weekly_announcement_guild(
            self, guild, completed_start):
        if not self._is_enabled(guild.id, AKARI_GAME.feature_flag):
            return False
        channel_id = cf_common.user_db.get_guild_config(
            guild.id, _AKARI_WEEKLY_POST_CHANNEL_KEY)
        if channel_id is None:
            return False

        week_key = completed_start.isoformat()
        kvs_key = f'{_AKARI_WEEKLY_POST_LAST_PREFIX}{guild.id}'
        if cf_common.user_db.kvs_get(kvs_key) == week_key:
            return False
        sent = await self._send_akari_weekly_announcement(
            guild, int(channel_id), completed_start)
        if sent:
            cf_common.user_db.kvs_set(kvs_key, week_key)
        return sent

    async def _send_akari_weekly_announcement(
            self, guild, channel_id, completed_start):
        try:
            channel = await self._resolve_channel(channel_id)
        except Exception:
            logger.warning(
                'Weekly Akari target missing for guild=%s channel=%s',
                guild.id, channel_id, exc_info=True)
            return False

        next_monday = completed_start + dt.timedelta(days=7)
        rating_rows, standings = await self._akari_weekly_snapshot(
            guild.id,
            as_of_date=next_monday,
            standings_date=completed_start,
        )
        # An empty week is handled once without posting, preventing a check
        # every five minutes for the rest of the following week.
        if not standings:
            logger.info(
                'No Weekly Akari results for guild=%s week=%s',
                guild.id, completed_start)
            return True

        registrants = cf_common.user_db.get_akari_registrants(guild.id)
        public_standings = [
            row for row in standings if row.user_id in registrants
        ]
        public_ratings = [
            row for row in rating_rows if row.user_id in registrants
        ]
        public_ratings = self._active_ranking_rows(
            public_ratings, include_inactive=False)
        if not public_standings:
            return True

        completed_end = completed_start + dt.timedelta(days=6)
        files = [_mg()._get_akari_weekly_table_image_file(
            guild, public_standings[:3],
            title=(f'Daily Akari Weekly Top 3 · {completed_start:%b %d}–'
                   f'{completed_end:%b %d}'),
        )]
        if public_ratings:
            files.append(_mg()._get_akari_rating_table_image_file(
                guild, public_ratings, registrants,
                title='Daily Akari Weekly Ratings',
                mark_registered=False,
                games_label='Weeks',
            ))
        await channel.send(
            f'🏆 **Daily Akari week complete · '
            f'{completed_start:%b %d}–{completed_end:%b %d}**\n'
            'Final top 3 and updated weekly ratings:',
            files=files,
        )
        return True

    @staticmethod
    async def _akari_difficulty_map(puzzle_numbers):
        puzzle_numbers = {
            int(number) for number in puzzle_numbers if int(number) > 0
        }
        cached = cf_common.user_db.get_akari_puzzle_difficulties(
            puzzle_numbers)
        missing = puzzle_numbers - set(cached)
        if not missing:
            return cached
        try:
            fetched = await asyncio.wait_for(
                fetch_akari_difficulties(missing),
                timeout=_AKARI_DIFFICULTY_FETCH_TIMEOUT,
            )
        except Exception:
            logger.warning('Could not refresh Daily Akari difficulties',
                           exc_info=True)
            return cached
        if fetched:
            cf_common.user_db.upsert_akari_puzzle_difficulties(
                fetched, time.time())
            cached.update(fetched)
        return cached
