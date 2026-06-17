import datetime
from collections import namedtuple

import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')

import discord
from discord.ext import commands

from tle.util import codeforces_api as cf
from tle.util import discord_common
from tle.util import paginator
from tle.util import table
# Shared ranking-table image renderer + colour/fonts; re-exported so existing
# tle.cogs.handles.rating_to_color / FONTS references keep working.
from tle.util.ranking_image import (  # noqa: F401
    FONTS, rating_to_color, render_ranking_table_image)

from PIL import Image, ImageDraw

_HANDLES_PER_PAGE = 15
_NAME_MAX_LEN = 20
_PAGINATE_WAIT_TIME = 5 * 60  # 5 minutes
_PRETTY_HANDLES_PER_PAGE = 10
_TOP_DELTAS_COUNT = 10
_MAX_RATING_CHANGES_PER_EMBED = 15
_UPDATE_HANDLE_STATUS_INTERVAL = 6 * 60 * 60  # 6 hours

_DIVISION_RATING_LOW = (2100, 1600, -1000)
_DIVISION_RATING_HIGH = (9999, 2099, 1599)
_LEADERBOARD_PER_PAGE = 10

_GudgitterRow = namedtuple('GudgitterRow', 'user_id handle rating score')


class HandleCogError(commands.CommandError):
    pass


def get_gudgitters_image(rankings):
    """return PIL image for rankings"""
    rows = [(str(pos + 1), name, handle, rating, str(score))
            for pos, name, handle, rating, score in rankings]
    return render_ranking_table_image(
        rows, headers=('#', 'Name', 'Handle', 'Points'),
        filename='gudgitters.png', width=900, rank_ratio=0.08, name_ratio=0.38)


def get_prettyhandles_image(rows, font):
    """return PIL image for rankings"""
    SMOKE_WHITE = (250, 250, 250)
    BLACK = (0, 0, 0)
    img = Image.new('RGB', (900, 450), color=SMOKE_WHITE)
    draw = ImageDraw.Draw(img)

    START_X, START_Y = 20, 20
    Y_INC = 32
    WIDTH_RANK = 64
    WIDTH_NAME = 340

    def draw_row(pos, username, handle, rating, color, y):
        x = START_X
        draw.text((x, y), pos, fill=color, font=font)
        x += WIDTH_RANK
        draw.text((x, y), username, fill=color, font=font)
        x += WIDTH_NAME
        draw.text((x, y), handle, fill=color, font=font)
        x += WIDTH_NAME
        draw.text((x, y), rating, fill=color, font=font)

    y = START_Y
    # draw header
    draw_row('#', 'Username', 'Handle', 'Rating', BLACK, y)
    y += int(Y_INC * 1.5)

    # trim name to fit in the column width
    def _trim(name):
        width = WIDTH_NAME - 10
        while font.getsize(name)[0] > width:
            name = name[:-4] + '...'  # "…" is printed as floating dots
        return name

    for pos, name, handle, rating in rows:
        name = _trim(name)
        handle = _trim(handle)
        color = rating_to_color(rating)
        draw_row(str(pos), name, handle, str(rating) if rating else 'N/A', color, y)
        if rating and rating >= 3000:  # nutella
            nutella_x = START_X + WIDTH_RANK
            draw.text((nutella_x, y), name[0], fill=BLACK, font=font)
            nutella_x += WIDTH_NAME
            draw.text((nutella_x, y), handle[0], fill=BLACK, font=font)
        y += Y_INC

    return img


def _make_profile_embed(member, user, *, mode):
    assert mode in ('set', 'get')
    if mode == 'set':
        desc = f'Handle for {member.mention} successfully set to **[{user.handle}]({user.url})**'
    else:
        desc = f'Handle for {member.mention} is currently set to **[{user.handle}]({user.url})**'
    if user.rating is None:
        embed = discord.Embed(description=desc)
        embed.add_field(name='Rating', value='Unrated', inline=True)
    else:
        embed = discord.Embed(description=desc, color=user.rank.color_embed)
        embed.add_field(name='Rating', value=user.rating, inline=True)
        embed.add_field(name='Rank', value=user.rank.title, inline=True)
    embed.set_thumbnail(url=f'{user.titlePhoto}')
    return embed


def _make_pages(users, title):
    chunks = paginator.chunkify(users, _HANDLES_PER_PAGE)
    pages = []
    done = 0

    style = table.Style('{:>}  {:<}  {:<}  {:<}')
    for chunk in chunks:
        t = table.Table(style)
        t += table.Header('#', 'Name', 'Handle', 'Rating')
        t += table.Line()
        for i, (member, handle, rating) in enumerate(chunk):
            name = member.display_name
            if len(name) > _NAME_MAX_LEN:
                name = name[:_NAME_MAX_LEN - 1] + '…'
            rank = cf.rating2rank(rating)
            rating_str = 'N/A' if rating is None else str(rating)
            t += table.Data(i + done, name, handle, f'{rating_str} ({rank.title_abbr})')
        table_str = '```\n'+str(t)+'\n```'
        embed = discord_common.cf_color_embed(description=table_str)
        pages.append((title, embed))
        done += len(chunk)
    return pages


def parse_date(arg):
    try:
        if len(arg) == 6:
            fmt = '%m%Y'
        # elif len(arg) == 4:
            # fmt = '%Y'
        else:
            raise ValueError
        return datetime.datetime.strptime(arg, fmt)
    except ValueError:
        raise HandleCogError(f'{arg} is an invalid date argument')


def _parse_gudgitter_args(args):
    division = None
    showall = False
    for arg in args:
        if arg[0:3] == 'div':
            try:
                division = int(arg[3])
                if division < 1 or division > 3:
                    raise HandleCogError('Division number must be within range [1-3]')
            except ValueError:
                raise HandleCogError(f'{arg} is an invalid div argument')
        if arg == '+all':
            showall = True
    return division, showall
