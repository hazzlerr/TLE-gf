"""Shared Cairo/Pango renderer for ranking-table PNGs.

Extracted from the handles and training cog helpers, which each re-implemented
the same surface setup + row drawing (only the width, column ratios, headers,
value column and filename differed). Callers pre-format each row and pass the
layout knobs; this module owns the drawing.
"""
import html
import io

import cairo
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo

import discord


FONTS = [
    'Noto Sans',
    'Noto Sans CJK JP',
    'Noto Sans CJK SC',
    'Noto Sans CJK TC',
    'Noto Sans CJK HK',
    'Noto Sans CJK KR',
    # extra/fonts.conf rejects Noto Color Emoji on old Cairo; fonts-color.conf
    # allows it only after startup verifies a compatible Cairo runtime.
    'Noto Color Emoji',
    'Noto Emoji',
]


def rating_to_color(rating):
    """returns (r, g, b) pixels values corresponding to rating"""
    # TODO: Integrate these colors with the ranks in codeforces_api.py
    BLACK = (10, 10, 10)
    RED = (255, 20, 20)
    BLUE = (0, 0, 200)
    GREEN = (0, 140, 0)
    ORANGE = (250, 140, 30)
    PURPLE = (160, 0, 120)
    CYAN = (0, 165, 170)
    GREY = (70, 70, 70)
    if rating is None or rating == 'N/A':
        return BLACK
    if rating < 1200:
        return GREY
    if rating < 1400:
        return GREEN
    if rating < 1600:
        return CYAN
    if rating < 1900:
        return BLUE
    if rating < 2100:
        return PURPLE
    if rating < 2400:
        return ORANGE
    return RED


def render_ranking_table_image(rows, *, headers, filename, width=900,
                               rank_ratio=0.08, name_ratio=0.38):
    """Render a ranking table to a ``discord.File`` PNG.

    ``rows``: iterable of ``(rank_text, name, handle, rating, value_text)``.
    ``headers``: 4-tuple ``(rank, name, handle, value)`` for the bold header row.
    The handle column is rendered as ``"<handle> (<rating or N/A>)"`` and the row
    colour comes from :func:`rating_to_color`; ratings >= 3000 get the nutella
    overdraw. ``width``/``rank_ratio``/``name_ratio`` tune the column geometry.
    """
    rows = list(rows)
    SMOKE_WHITE = (250, 250, 250)
    BLACK = (0, 0, 0)

    DISCORD_GRAY = (.212, .244, .247)

    ROW_COLORS = ((0.95, 0.95, 0.95), (0.9, 0.9, 0.9))

    WIDTH = width
    BORDER_MARGIN = 20
    COLUMN_MARGIN = 10
    HEADER_SPACING = 1.25
    WIDTH_RANK = rank_ratio * WIDTH
    WIDTH_NAME = name_ratio * WIDTH
    LINE_HEIGHT = 40
    HEIGHT = int((len(rows) + HEADER_SPACING) * LINE_HEIGHT + 2 * BORDER_MARGIN)
    # Cairo+Pango setup
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, WIDTH, HEIGHT)
    context = cairo.Context(surface)
    context.set_line_width(1)
    context.set_source_rgb(*DISCORD_GRAY)
    context.rectangle(0, 0, WIDTH, HEIGHT)
    context.fill()
    layout = PangoCairo.create_layout(context)
    layout.set_font_description(Pango.font_description_from_string(','.join(FONTS) + ' 20'))
    layout.set_ellipsize(Pango.EllipsizeMode.END)

    def draw_bg(y, color_index):
        nxty = y + LINE_HEIGHT

        # Simple
        context.move_to(BORDER_MARGIN, y)
        context.line_to(WIDTH, y)
        context.line_to(WIDTH, nxty)
        context.line_to(0, nxty)
        context.set_source_rgb(*ROW_COLORS[color_index])
        context.fill()

    def draw_row(pos, username, handle, rating, color, y, bold=False):
        context.set_source_rgb(*[x / 255.0 for x in color])

        context.move_to(BORDER_MARGIN, y)

        def draw(text, width=-1):
            text = html.escape(text)
            if bold:
                text = f'<b>{text}</b>'
            layout.set_width((width - COLUMN_MARGIN) * 1000)  # pixel = 1000 pango units
            layout.set_markup(text, -1)
            PangoCairo.show_layout(context, layout)
            context.rel_move_to(width, 0)

        draw(pos, WIDTH_RANK)
        draw(username, WIDTH_NAME)
        draw(handle, WIDTH_NAME)
        draw(rating)

    y = BORDER_MARGIN

    # draw header
    draw_row(headers[0], headers[1], headers[2], headers[3], SMOKE_WHITE, y, bold=True)
    y += LINE_HEIGHT * HEADER_SPACING

    for i, (rank_text, name, handle, rating, value_text) in enumerate(rows):
        color = rating_to_color(rating)
        draw_bg(y, i % 2)
        handle_text = f'{handle} ({rating if rating else "N/A"})'
        draw_row(rank_text, f'{name}', handle_text, value_text, color, y)
        if rating and rating >= 3000:  # nutella
            draw_row('', name[0], handle[0], '', BLACK, y)
        y += LINE_HEIGHT

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    return discord.File(image_data, filename=filename)
