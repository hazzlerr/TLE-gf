"""Current-week Akari score table rendering."""

from tle.cogs._minigame_helpers import _mg, _safe_cf_handle, _safe_user_name
from tle.cogs._minigame_tables import _AKARI_IMAGE_MAX_ROWS


_AKARI_WEEKLY_COLS = (54, 360, 340, 106)


def _akari_weekly_table_rows(guild, standings, *, identity_fn=None):
    """Compact current-week rows: rank, player, handle, rounded score."""
    if identity_fn is None:
        identity_fn = lambda g, row: _safe_cf_handle(g, row.user_id)
    rows = []
    previous_score = None
    rank = 0
    for index, standing in enumerate(standings, start=1):
        rounded_score = round(standing.score * 1000)
        if previous_score is None or rounded_score != previous_score:
            rank = index
            previous_score = rounded_score
        rows.append((
            rank,
            _safe_user_name(guild, standing.user_id),
            identity_fn(guild, standing),
            rounded_score,
        ))
    return rows


def _get_akari_weekly_table_image_file(guild, standings, *, title):
    displayed = standings[:_AKARI_IMAGE_MAX_ROWS]
    table_rows = _akari_weekly_table_rows(guild, displayed)
    footer = None
    if len(standings) > len(displayed):
        footer = f'Showing top {len(displayed)} of {len(standings)} players'
    return _mg()._get_akari_puzzle_table_image(
        table_rows,
        title=title,
        footer=footer,
        header=('#', 'Player', 'Handle', 'Score'),
        cols=_AKARI_WEEKLY_COLS,
        right_align_cols=(0, 3),
        filename='akari-weekly-scores.png',
    )
