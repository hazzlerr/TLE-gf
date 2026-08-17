"""Pure parsing helpers for counting-channel messages.

An unprefixed token is compared with the canonical decimal, binary, and
hexadecimal spelling of the expected count.  Binary and hexadecimal may also
use ``0b`` and ``0x`` prefixes.  The expected value disambiguates spellings
such as ``10``: it is binary two, decimal ten, or hexadecimal sixteen
depending on which number is currently due.

Letter-only hexadecimal is necessarily ambiguous with ordinary prose.  Such
a token is accepted when it exactly spells the expected value (for example
``A`` for ten), but otherwise ignored instead of marking normal chat wrong.
"""

import re
from dataclasses import dataclass
from typing import Optional


CORRECT = 'correct'
WRONG_NUMBER = 'wrong_number'
INVALID_FORMAT = 'invalid_format'
IGNORED = 'ignored'

_HEX_TOKEN_RE = re.compile(r'^[0-9a-f]+$', re.IGNORECASE)
_DECIMAL_SHAPE_RE = re.compile(r'^(?:[0-9]+\.[0-9]*|\.[0-9]+)$')


@dataclass(frozen=True)
class CountAttempt:
    """Classification of one possible counting message.

    ``radix`` is populated whenever the representation is unambiguous to the
    parser: for a correct count, and for a canonical prefixed value.  Bare
    wrong values can describe different numbers in several radices, so their
    radix and value remain ``None``.
    """

    status: str
    radix: Optional[int] = None
    value: Optional[int] = None

    @property
    def is_correct(self):
        return self.status == CORRECT

    @property
    def is_bad_attempt(self):
        return self.status in (WRONG_NUMBER, INVALID_FORMAT)

    @property
    def reason(self):
        """Machine-readable rejection reason, or ``None`` when not rejected."""
        return self.status if self.is_bad_attempt else None


def _single_token(content):
    if not isinstance(content, str):
        return None
    token = content.strip()
    if not token or any(char.isspace() for char in token):
        return None
    return token


def could_be_count_attempt(content):
    """Cheap expected-independent filter for the global message listener.

    A standalone token reaches the DB-backed classifier when it contains a
    digit (including malformed numeric shapes such as ``22.3``), or when it
    consists only of A-F and could therefore be a canonical bare hex count.
    Ordinary prose and multi-token chat are filtered out.
    """
    token = _single_token(content)
    if token is None:
        return False
    if any(char.isdigit() for char in token):
        return True
    return all(char in 'abcdefABCDEF' for char in token)


def _correct_radix(token, expected):
    lowered = token.lower()
    decimal = str(expected)
    binary = format(expected, 'b')
    hexadecimal = format(expected, 'x')

    # Prefer decimal for the small values whose bare spellings overlap.
    if lowered == decimal:
        return 10
    if lowered == binary:
        return 2
    if lowered == hexadecimal:
        return 16
    if lowered == '0b' + binary:
        return 2
    if lowered == '0x' + hexadecimal:
        return 16
    return None


def _classify_prefixed(token, prefix, radix, valid_digits, format_code):
    lowered = token.lower()
    body = lowered[len(prefix):]
    if not body or any(char not in valid_digits for char in body):
        return CountAttempt(INVALID_FORMAT)
    value = int(body, radix)
    if body != format(value, format_code):
        return CountAttempt(INVALID_FORMAT)
    return CountAttempt(WRONG_NUMBER, radix=radix, value=value)


def _is_canonical_bare(token, radix, valid_digits, format_code):
    lowered = token.lower()
    if not lowered or any(char not in valid_digits for char in lowered):
        return False
    value = int(lowered, radix)
    return lowered == format(value, format_code)


def classify_count_attempt(content, expected):
    """Classify *content* against the non-negative integer *expected*.

    The result distinguishes messages the counting cog should ignore from bad
    attempts that deserve its negative reaction.  Only canonical spellings
    are accepted. Leading zeroes, decimal-shaped values, and malformed base
    prefixes are invalid; unrelated punctuated or alphanumeric chat is ignored.
    """
    if isinstance(expected, bool) or not isinstance(expected, int):
        raise TypeError('expected must be an integer')
    if expected < 0:
        raise ValueError('expected must be non-negative')

    token = _single_token(content)
    if token is None:
        return CountAttempt(IGNORED)

    radix = _correct_radix(token, expected)
    if radix is not None:
        return CountAttempt(CORRECT, radix=radix, value=expected)

    lowered = token.lower()
    if lowered.startswith('0b'):
        return _classify_prefixed(token, '0b', 2, '01', 'b')
    if lowered.startswith('0x'):
        return _classify_prefixed(
            token, '0x', 16, '0123456789abcdef', 'x')

    # A-F words could be either a count or ordinary chat.  Unless one matched
    # the expected canonical hex form above, leave it alone.
    if token.isalpha():
        return CountAttempt(IGNORED)

    if _HEX_TOKEN_RE.fullmatch(token):
        canonical_somewhere = (
            _is_canonical_bare(token, 10, '0123456789', 'd')
            or _is_canonical_bare(token, 2, '01', 'b')
            or _is_canonical_bare(
                token, 16, '0123456789abcdef', 'x')
        )
        return CountAttempt(
            WRONG_NUMBER if canonical_somewhere else INVALID_FORMAT)

    # A single decimal point still looks like a standalone numeric attempt,
    # but unrelated digit-bearing tokens (dates, filenames, ``2nd``) are chat.
    if _DECIMAL_SHAPE_RE.fullmatch(token):
        return CountAttempt(INVALID_FORMAT)
    return CountAttempt(IGNORED)
