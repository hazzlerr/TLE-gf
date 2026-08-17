"""Pure parsing helpers for counting-channel messages.

A canonical decimal, binary, or hexadecimal spelling of the expected count
may appear anywhere in a message.  Binary and hexadecimal may also use ``0b``
and ``0x`` prefixes.  Numeric spellings use literal substring matching.
Multi-letter bare hexadecimal requires token boundaries, while a single bare
hex letter must be the entire message, so normal prose does not advance it.
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
_MULTI_LETTER_HEX_CANDIDATE_RE = re.compile(
    r'(?<![0-9a-z_])[a-f]{2,}(?![0-9a-z_])', re.IGNORECASE)


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


def _stripped_message(content):
    if not isinstance(content, str):
        return None
    stripped = content.strip()
    return stripped or None


def could_be_count_attempt(content):
    """Cheap expected-independent filter for the global message listener.

    A message reaches the DB-backed classifier when it contains a digit, an
    all-A-F token of at least two letters, or consists solely of one A-F
    letter.  The full classifier decides whether that text is the expected
    value, a bad numeric attempt, or unrelated prose.
    """
    if not isinstance(content, str) or not content.strip():
        return False
    if any(char.isdigit() for char in content):
        return True
    stripped = content.strip()
    if len(stripped) == 1 and stripped.lower() in 'abcdef':
        return True
    return _MULTI_LETTER_HEX_CANDIDATE_RE.search(content) is not None


def _correct_radix(content, expected):
    lowered = content.lower()
    stripped = lowered.strip()
    decimal = str(expected)
    binary = format(expected, 'b')
    hexadecimal = format(expected, 'x')

    # Pick the earliest representation in the message.  At the same position,
    # an explicit prefix wins over the bare digits it contains; otherwise keep
    # the traditional decimal, binary, hexadecimal preference.
    representations = (
        ('0b' + binary, 2, 0),
        ('0x' + hexadecimal, 16, 1),
        (decimal, 10, 2),
        (binary, 2, 3),
        (hexadecimal, 16, 4),
    )
    matches = []
    for spelling, radix, preference in representations:
        if spelling.isalpha():
            if len(spelling) == 1:
                start = 0 if stripped == spelling else -1
            else:
                pattern = re.compile(
                    rf'(?<![0-9a-z_]){re.escape(spelling)}(?![0-9a-z_])')
                match = pattern.search(lowered)
                start = -1 if match is None else match.start()
        else:
            start = lowered.find(spelling)
        if start >= 0:
            matches.append((start, -len(spelling), preference, radix))

    return min(matches)[-1] if matches else None


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
    attempts that deserve its negative reaction.  The matching substring must
    be canonical; surrounding text may be arbitrary.  When no match exists,
    standalone leading-zero values, decimal shapes, and malformed base
    prefixes are invalid, while unrelated alphanumeric chat is ignored.
    """
    if isinstance(expected, bool) or not isinstance(expected, int):
        raise TypeError('expected must be an integer')
    if expected < 0:
        raise ValueError('expected must be non-negative')

    stripped = _stripped_message(content)
    if stripped is None:
        return CountAttempt(IGNORED)

    radix = _correct_radix(content, expected)
    if radix is not None:
        return CountAttempt(CORRECT, radix=radix, value=expected)

    return _classify_bad_message(stripped)


def _classify_bad_message(content):
    """Reject only a whole non-matching message that looks numeric."""
    lowered = content.lower()
    if lowered.startswith('0b'):
        return _classify_prefixed(content, '0b', 2, '01', 'b')
    if lowered.startswith('0x'):
        return _classify_prefixed(
            content, '0x', 16, '0123456789abcdef', 'x')

    # A-F words could be either a count or ordinary chat.  Unless one matched
    # the expected canonical hex form above, leave it alone.
    if content.isalpha():
        return CountAttempt(IGNORED)

    if _HEX_TOKEN_RE.fullmatch(content):
        canonical_somewhere = (
            _is_canonical_bare(content, 10, '0123456789', 'd')
            or _is_canonical_bare(content, 2, '01', 'b')
            or _is_canonical_bare(
                content, 16, '0123456789abcdef', 'x')
        )
        return CountAttempt(
            WRONG_NUMBER if canonical_somewhere else INVALID_FORMAT)

    # A single decimal point still looks like a standalone numeric attempt,
    # but unrelated digit-bearing tokens (dates, filenames, ``2nd``) are chat.
    if _DECIMAL_SHAPE_RE.fullmatch(content):
        return CountAttempt(INVALID_FORMAT)
    return CountAttempt(IGNORED)
