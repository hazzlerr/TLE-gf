import logging
import os
import re
from pathlib import Path


logger = logging.getLogger(__name__)

_MIN_COLOR_CAIRO_VERSION = (1, 18, 0)
_TRUE_VALUES = {'1', 'true', 'yes', 'on'}
_STATUS = {
    'requested': False,
    'enabled': False,
    'cairo_version': None,
    'fontconfig_file': None,
    'reason': 'not configured',
}


def _repo_root():
    return Path(__file__).resolve().parents[2]


def _fontconfig_path(filename):
    return str(_repo_root() / 'extra' / filename)


def _parse_version(version):
    parts = [int(part) for part in re.findall(r'\d+', version or '')[:3]]
    return tuple((parts + [0, 0, 0])[:3])


def _version_at_least(version, minimum):
    return _parse_version(version) >= minimum


def _cairo_version():
    try:
        import cairo
    except Exception as e:
        return None, e

    try:
        return cairo.cairo_version_string(), None
    except Exception as e:
        return None, e


def configure():
    """Select a fontconfig file before Pango/fontconfig are initialized."""
    global _STATUS

    requested = os.environ.get('TLE_ALLOW_COLOR_EMOJI', '').lower() in _TRUE_VALUES
    enabled = False
    cairo_version = None
    reason = 'color emoji not requested'

    if requested:
        cairo_version, error = _cairo_version()
        if cairo_version is None:
            reason = f'could not determine Cairo version: {error}'
        elif _version_at_least(cairo_version, _MIN_COLOR_CAIRO_VERSION):
            enabled = True
            reason = 'color emoji enabled'
        else:
            reason = f'Cairo {cairo_version} is older than 1.18.0'

    fontconfig_file = _fontconfig_path('fonts-color.conf' if enabled else 'fonts.conf')
    os.environ['FONTCONFIG_FILE'] = fontconfig_file
    os.environ['TLE_COLOR_EMOJI_ENABLED'] = '1' if enabled else '0'

    _STATUS = {
        'requested': requested,
        'enabled': enabled,
        'cairo_version': cairo_version,
        'fontconfig_file': fontconfig_file,
        'reason': reason,
    }
    return enabled


def status():
    return _STATUS.copy()


def log_status():
    current = status()
    if current['enabled']:
        logger.info(
            'Color emoji fontconfig enabled with Cairo %s.',
            current['cairo_version'],
        )
    elif current['requested']:
        logger.warning(
            'Color emoji requested but disabled (%s); using monochrome emoji.',
            current['reason'],
        )
    else:
        logger.info('Using monochrome emoji fontconfig.')
