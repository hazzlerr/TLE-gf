"""Signup/signout event helpers for the Great Day cog.

The ``greatday_signup`` table only stores current membership, so joins and
leaves live in ``greatday_signup_event``. Live commands record an event as
they run; :func:`scan_signup_events` recovers the events that happened before
the ledger existed by replaying a channel's message history.
"""
import logging
import re
from collections import deque

from tle.cogs._greatday_helpers import _format_pick_time

logger = logging.getLogger(__name__)

# `;greatday signup`, `@TLE greatday remove`, `;greatday add @user`, …
_SIGNUP_COMMAND_RE = re.compile(
    r'^(?:<@!?\d+>\s*)?;?\s*greatday\s+(signup|remove|add|kick)\b(?P<rest>.*)$',
    re.IGNORECASE | re.DOTALL)
_MENTION_RE = re.compile(r'<@!?(\d+)>')

# Which action each command records once the bot confirms it took effect.
_COMMAND_ACTIONS = {
    'signup': 'signup',
    'add': 'signup',
    'remove': 'signout',
    'kick': 'signout',
}
# The command's target: the invoker, or the member named in an admin command.
_TARGETS_MENTION = frozenset(('add', 'kick'))

# Fragments of the cog's own success embeds. A command that failed (already
# signed up, banned, missing role) never produces these, which keeps the
# backfill from inventing events that never happened.
_SUCCESS_FRAGMENTS = {
    'signup': ('signed up for great day pings', 'added to great day pings'),
    'signout': ('removed from great day pings',),
}
# How many newer messages may sit between a command and the bot's reply.
_REPLY_LOOKAHEAD = 4


def embed_texts(msg):
    """Yield the text of a message and of its embeds."""
    yield msg.content or ''
    for embed in getattr(msg, 'embeds', None) or ():
        yield getattr(embed, 'description', None) or ''
        yield getattr(embed, 'title', None) or ''


def is_success_reply(msg, bot_user_id, action):
    """Whether ``msg`` is the bot's confirmation of ``action``."""
    author_id = getattr(getattr(msg, 'author', None), 'id', None)
    if author_id != bot_user_id:
        return False
    fragments = _SUCCESS_FRAGMENTS[action]
    return any(fragment in text.lower()
               for text in embed_texts(msg)
               for fragment in fragments)


def parse_signup_command(msg):
    """Return ``(action, user_id)`` for a Great Day membership command.

    ``None`` when the message is not one, or when an admin command does not
    name a member.
    """
    match = _SIGNUP_COMMAND_RE.match((msg.content or '').strip())
    if match is None:
        return None
    command = match.group(1).lower()
    action = _COMMAND_ACTIONS[command]
    if command in _TARGETS_MENTION:
        mentions = _MENTION_RE.findall(match.group('rest'))
        if not mentions:
            return None
        return action, mentions[0]
    author_id = getattr(getattr(msg, 'author', None), 'id', None)
    if author_id is None:
        return None
    return action, str(author_id)


def record_event(db, guild_id, user_id, action, message):
    """Record one event, best-effort.

    Membership already changed by the time this runs, so a logging failure
    must not turn a successful command into an error reply.
    """
    try:
        db.greatday_record_signup_event(
            guild_id, user_id, action, message.created_at.timestamp(),
            message.id)
    except Exception:
        logger.exception('Failed to record greatday %s event for guild=%s '
                         'user=%s', action, guild_id, user_id)


async def scan_signup_events(channel, guild_id, bot_user_id, progress=None,
                             progress_interval=250):
    """Replay ``channel`` and return ``(scanned, events)``.

    ``events`` are ``(guild_id, user_id, action, at, message_id)`` tuples for
    commands the bot confirmed, keyed by the invoking message so re-running
    the scan inserts nothing new. Bans are skipped: a ban removes a signup
    silently, so history cannot tell whether the user was signed up.
    """
    scanned = 0
    events = []
    # Newest-first, so the messages already seen are the *newer* ones the
    # bot's reply would be among.
    newer = deque(maxlen=_REPLY_LOOKAHEAD)
    async for msg in channel.history(limit=None, oldest_first=False):
        scanned += 1
        parsed = parse_signup_command(msg)
        if parsed is not None:
            action, user_id = parsed
            if any(is_success_reply(reply, bot_user_id, action)
                   for reply in newer):
                events.append((guild_id, user_id, action,
                               msg.created_at.timestamp(), msg.id))
        newer.appendleft(msg)
        if progress is not None and scanned % progress_interval == 0:
            await progress(scanned, len(events))
    return scanned, events


def signed_up_post_count(events, post_times, currently_signed_up):
    """Count Great Day posts that happened while the user was signed up.

    ``events`` may be in any order; ``post_times`` are guild-wide post
    timestamps. Returns ``(count, complete)``. ``complete`` is False when the
    ledger cannot cover the user's whole membership — a signout with no
    preceding signup, or current membership with no events at all, both mean
    they joined before the ledger existed, so the count is a lower bound.
    """
    ordered = sorted(events, key=lambda row: (row.at, str(row.message_id)))
    intervals = []
    start = None
    complete = True
    for row in ordered:
        if row.action == 'signup':
            if start is None:
                start = row.at
        elif start is not None:
            intervals.append((start, row.at))
            start = None
        else:
            # Left without a recorded join.
            complete = False
    if start is not None:
        intervals.append((start, float('inf')))
    elif currently_signed_up and not ordered:
        complete = False
    count = sum(1 for at in post_times
                if any(lo <= at <= hi for lo, hi in intervals))
    return count, complete


def collapse_events(events):
    """Drop events that do not change signed-up state.

    A second signup while already signed up (or a repeated signout) carries no
    information, so only state transitions are kept. Accepts events in any
    order and returns the kept ones newest first.
    """
    ordered = sorted(events, key=lambda row: (row.at, str(row.message_id)))
    kept = []
    for row in ordered:
        if not kept or kept[-1].action != row.action:
            kept.append(row)
    kept.reverse()
    return kept


def merge_history(picks, events):
    """Interleave picks and signup events into newest-first display lines.

    Picks keep their bare timestamp line; only signup/signout are labelled.
    """
    labels = {'signup': 'Signed up — ', 'signout': 'Signed out — '}
    entries = [(row.picked_at, str(row.message_id), '') for row in picks]
    entries += [(row.at, str(row.message_id), labels[row.action])
                for row in events]
    # Ties break on message id, matching the DB queries' ordering.
    entries.sort(key=lambda e: (e[0], int(e[1]) if e[1].isdigit() else 0),
                 reverse=True)
    return [f'{label}{_format_pick_time(at)}' for at, _, label in entries]
