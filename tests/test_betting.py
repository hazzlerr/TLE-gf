"""Tests for the soccer betting minigame — pure helpers, the odds-API
parsers, the wallet/market/wager DB layer, and the cog's bet-execution path."""
import sqlite3
from datetime import datetime, timezone

import pytest

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import upgrade_1_33_0
from tle.util import odds_api
from tle.cogs.betting import (
    outcome_from_score, payout_amount, normalize_pick, parse_amount,
    parse_bet_message, parse_settle_arg, rank_line,
)


# ── Pure helpers ───────────────────────────────────────────────────────────

class TestOutcome:
    def test_home_win(self):
        assert outcome_from_score(2, 1) == 'home'

    def test_away_win(self):
        assert outcome_from_score(0, 3) == 'away'

    def test_draw(self):
        assert outcome_from_score(1, 1) == 'draw'
        assert outcome_from_score(0, 0) == 'draw'


class TestPayout:
    def test_basic(self):
        assert payout_amount(100, 2.5) == 250

    def test_rounding(self):
        assert payout_amount(100, 1.555) == 156  # round-half handled by round()

    def test_decimal_odds_one_returns_stake(self):
        assert payout_amount(50, 1.0) == 50


class TestNormalizePick:
    @pytest.mark.parametrize('text,expected', [
        ('home', 'home'), ('H', 'home'), ('1', 'home'),
        ('draw', 'draw'), ('x', 'draw'), ('tie', 'draw'),
        ('away', 'away'), ('A', 'away'), ('2', 'away'),
        ('  Home  ', 'home'),
    ])
    def test_aliases(self, text, expected):
        assert normalize_pick(text) == expected

    def test_unknown(self):
        assert normalize_pick('banana') is None
        assert normalize_pick(None) is None


class TestParseAmount:
    def test_plain_number(self):
        assert parse_amount('100', 500) == 100

    def test_all(self):
        assert parse_amount('all', 500) == 500
        assert parse_amount('max', 350) == 350

    def test_all_with_zero_balance_is_none(self):
        assert parse_amount('all', 0) is None

    def test_percentage(self):
        assert parse_amount('50%', 500) == 250
        assert parse_amount('10%', 95) == 9  # int() floors

    def test_percentage_out_of_range(self):
        assert parse_amount('0%', 500) is None
        assert parse_amount('150%', 500) is None

    def test_below_min(self):
        assert parse_amount('0', 500) is None
        assert parse_amount('-5', 500) is None

    def test_does_not_clamp_to_balance(self):
        # Over-balance plain numbers parse fine; the caller enforces the cap.
        assert parse_amount('999', 100) == 999

    def test_garbage(self):
        assert parse_amount('abc', 500) is None
        assert parse_amount('', 500) is None
        assert parse_amount(None, 500) is None


class TestParseBetMessage:
    def test_pick_then_amount(self):
        assert parse_bet_message('home 100') == ('home', '100')

    def test_amount_then_pick(self):
        assert parse_bet_message('100 away') == ('away', '100')

    def test_aliases_and_percent(self):
        assert parse_bet_message('x 50%') == ('draw', '50%')
        assert parse_bet_message('away all') == ('away', 'all')
        assert parse_bet_message('1 250') == ('home', '250')

    def test_ignores_ordinary_chat(self):
        assert parse_bet_message('lets go spain') is None
        assert parse_bet_message('home') is None
        assert parse_bet_message('home 100 now') is None
        assert parse_bet_message('home home') is None
        assert parse_bet_message('') is None
        assert parse_bet_message(None) is None


class TestParseSettleArg:
    def test_pick_words(self):
        assert parse_settle_arg('home') == ('home', None, None)
        assert parse_settle_arg('draw') == ('draw', None, None)
        assert parse_settle_arg('away') == ('away', None, None)

    def test_scoreline(self):
        assert parse_settle_arg('2-1') == ('home', 2, 1)
        assert parse_settle_arg('0:3') == ('away', 0, 3)
        assert parse_settle_arg('1-1') == ('draw', 1, 1)

    def test_invalid(self):
        assert parse_settle_arg('banana') is None
        assert parse_settle_arg('2-x') is None
        assert parse_settle_arg('-1--2') is None
        assert parse_settle_arg(None) is None


class TestRankLine:
    def _rows(self, pairs):
        from collections import namedtuple
        R = namedtuple('R', 'user_id balance')
        return [R(str(u), b) for u, b in pairs]

    def test_found(self):
        rows = self._rows([(1, 900), (2, 500)])
        line = rank_line(rows, 2, 'balance', 'wallet')
        assert '#2' in line and '500' in line

    def test_user_id_coercion(self):
        rows = self._rows([(100, 10)])
        assert '#1' in rank_line(rows, 100, 'balance', 'wallet')
        assert '#1' in rank_line(rows, '100', 'balance', 'wallet')

    def test_not_found(self):
        rows = self._rows([(1, 900)])
        assert 'not on' in rank_line(rows, 999, 'balance', 'wallet')


# ── Odds API parsers ───────────────────────────────────────────────────────

class TestIsoToUnix:
    def test_z_suffix(self):
        expected = datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()
        assert odds_api.iso_to_unix('2026-06-20T15:00:00Z') == expected

    def test_offset(self):
        expected = datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()
        assert odds_api.iso_to_unix('2026-06-20T16:00:00+01:00') == expected

    def test_naive_treated_as_utc(self):
        expected = datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()
        assert odds_api.iso_to_unix('2026-06-20T15:00:00') == expected


def _raw_event(**over):
    raw = {
        'id': 'evt1', 'sport_key': 'soccer_epl',
        'commence_time': '2026-06-20T15:00:00Z',
        'home_team': 'Spain', 'away_team': 'Cape Verde',
        'bookmakers': [
            {'key': 'b1', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.5},
                {'name': 'Cape Verde', 'price': 6.0},
                {'name': 'Draw', 'price': 4.0}]}]},
            {'key': 'b2', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.6},
                {'name': 'Cape Verde', 'price': 6.5},
                {'name': 'Draw', 'price': 4.2}]}]},
        ],
    }
    raw.update(over)
    return raw


class TestParseH2H:
    def test_averages_across_bookmakers(self):
        parsed = odds_api.parse_h2h_event(_raw_event())
        assert parsed['event_id'] == 'evt1'
        assert parsed['home_team'] == 'Spain'
        assert parsed['away_team'] == 'Cape Verde'
        assert parsed['odds']['home'] == 1.55
        assert parsed['odds']['away'] == 6.25
        assert parsed['odds']['draw'] == 4.1
        assert parsed['commence_time'] == \
            datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()

    def test_missing_market_returns_none(self):
        assert odds_api.parse_h2h_event(_raw_event(bookmakers=[])) is None

    def test_partial_market_returns_none(self):
        # A book that only priced home/away (no draw) → incomplete 1X2.
        raw = _raw_event(bookmakers=[
            {'key': 'b', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.5},
                {'name': 'Cape Verde', 'price': 6.0}]}]}])
        assert odds_api.parse_h2h_event(raw) is None

    def test_missing_teams_returns_none(self):
        assert odds_api.parse_h2h_event(_raw_event(home_team=None)) is None


class TestParseScore:
    def test_completed_with_scores(self):
        raw = {'id': 'evt1', 'completed': True,
               'home_team': 'Spain', 'away_team': 'Cape Verde',
               'scores': [{'name': 'Spain', 'score': '2'},
                          {'name': 'Cape Verde', 'score': '1'}]}
        p = odds_api.parse_score_event(raw)
        assert p == {'event_id': 'evt1', 'completed': True,
                     'home_score': 2, 'away_score': 1}

    def test_not_completed(self):
        raw = {'id': 'evt1', 'completed': False}
        p = odds_api.parse_score_event(raw)
        assert p['completed'] is False
        assert p['home_score'] is None

    def test_completed_but_missing_score(self):
        raw = {'id': 'evt1', 'completed': True,
               'home_team': 'Spain', 'away_team': 'Cape Verde',
               'scores': [{'name': 'Spain', 'score': '2'}]}
        p = odds_api.parse_score_event(raw)
        assert p['completed'] is True
        assert p['home_score'] is None


# ── DB layer ───────────────────────────────────────────────────────────────

GUILD = '111'
CH = '222'
THREAD = '333'
USER_A = '100'
USER_B = '200'


@pytest.fixture
def db():
    return UserDbConn(':memory:')


def _make_market(db, commence=10_000.0, odds=(2.0, 3.0, 4.0)):
    mid = db.bet_market_create(
        GUILD, CH, 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', commence,
        odds[0], odds[1], odds[2], USER_A, 0.0)
    return mid


class TestWallet:
    def test_ensure_creates_at_start(self, db):
        assert db.bet_get_balance(GUILD, USER_A) is None
        assert db.bet_ensure_wallet(GUILD, USER_A, 1000) == 1000
        assert db.bet_get_balance(GUILD, USER_A) == 1000

    def test_ensure_idempotent(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        # second call must not reset a changed balance
        db.conn.execute('UPDATE bet_wallet SET balance = 50 WHERE user_id = ?',
                        (USER_A,))
        assert db.bet_ensure_wallet(GUILD, USER_A, 1000) == 50

    def test_guild_isolation(self, db):
        db.bet_ensure_wallet('1', USER_A, 1000)
        assert db.bet_get_balance('2', USER_A) is None


class TestDaily:
    def test_grants_once(self, db):
        granted, bal, reason = db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        assert granted is True and bal == 1100 and reason == 'ok'

    def test_second_claim_same_day_refused(self, db):
        db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        granted, bal, reason = db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        assert granted is False and bal == 1100 and reason == 'already'

    def test_next_day_grants_again(self, db):
        db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        granted, bal, _ = db.bet_claim_daily(GUILD, USER_A, '2026-06-16', 100, 1000)
        assert granted is True and bal == 1200


class TestMarket:
    def test_create_and_get(self, db):
        mid = _make_market(db)
        m = db.bet_market_get(mid)
        assert m.home_team == 'Spain'
        assert m.status == 'open'
        assert m.odds_home == 2.0

    def test_active_lookup(self, db):
        mid = _make_market(db)
        assert db.bet_market_get_active(GUILD, CH).market_id == mid
        assert db.bet_market_get_active(GUILD, 'other') is None

    def test_thread_lookup(self, db):
        mid = _make_market(db)
        db.bet_market_set_thread(mid, THREAD)
        assert db.bet_market_get_active_by_thread(GUILD, THREAD).market_id == mid
        assert db.bet_market_get_active_by_thread(GUILD, 'nope') is None

    def test_exists_open_for_event(self, db):
        _make_market(db)
        assert db.bet_market_exists_open_for_event(GUILD, 'evt1') is True
        assert db.bet_market_exists_open_for_event(GUILD, 'evtX') is False

    def test_pending_settlement_by_cutoff(self, db):
        mid = _make_market(db, commence=1000.0)
        assert [m.market_id for m in db.bet_markets_pending_settlement(2000.0)] == [mid]
        assert db.bet_markets_pending_settlement(500.0) == []


class TestPlaceBet:
    def test_escrow_deducts(self, db):
        mid = _make_market(db)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 300, 2.0, 1.0, 1000)
        assert ok and reason == 'ok' and bal == 700
        assert db.bet_get_balance(GUILD, USER_A) == 700

    def test_insufficient_balance(self, db):
        mid = _make_market(db)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 5000, 2.0, 1.0, 1000)
        assert ok is False and reason == 'insufficient' and bal == 1000
        # no wager recorded, balance untouched
        assert db.bet_get_wager(mid, USER_A) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1000

    def test_rebet_refunds_previous_then_charges(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 2.0, 1.0, 1000)  # bal 700
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'away', 200, 4.0, 2.0, 1000)
        assert ok and bal == 800  # 700 + 300 refund - 200
        w = db.bet_get_wager(mid, USER_A)
        assert w.pick == 'away' and w.stake == 200 and w.odds == 4.0

    def test_rebet_to_larger_stake_within_refunded_budget(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 1000, 2.0, 1.0, 1000)  # all-in, bal 0
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'home', 1000, 2.0, 2.0, 1000)
        assert ok and bal == 0  # refund 1000 then stake 1000 again


class TestPool:
    def test_pool_groups_by_pick(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 2.0, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'home', 200, 2.0, 1.0, 1000)
        pool = {p.pick: (p.cnt, p.total) for p in db.bet_pool(mid)}
        assert pool['home'] == (2, 300)


class TestSettle:
    def test_winner_credited_loser_zero(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 2.0, 1.0, 1000)  # bal 900
        db.bet_place(GUILD, mid, USER_B, 'away', 100, 4.0, 1.0, 1000)  # bal 900
        rows = db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        by_user = {r[0]: r for r in rows}
        assert by_user[USER_A][4] == 200  # payout 100*2.0
        assert by_user[USER_B][4] == 0
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # 900 + 200
        assert db.bet_get_balance(GUILD, USER_B) == 900   # unchanged
        assert db.bet_market_get(mid).status == 'settled'
        assert db.bet_market_get(mid).result == 'home'

    def test_draw_outcome(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'draw', 100, 3.0, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'draw', 1, 1, 5.0)
        assert db.bet_get_balance(GUILD, USER_A) == 1200  # 900 + 300

    def test_profit_leaderboard(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 2.0, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'away', 100, 4.0, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        prof = {r.user_id: (r.profit, r.bets, r.wins)
                for r in db.bet_profit_leaderboard(GUILD)}
        assert prof[USER_A] == (100, 1, 1)   # +200 payout - 100 stake
        assert prof[USER_B] == (-100, 1, 0)

    def test_double_settle_is_noop(self, db):
        """The status='open' guard must make a second settle pay nobody."""
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 2.0, 1.0, 1000)  # bal 900
        first = db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        assert first is not None
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # 900 + 200
        second = db.bet_settle(GUILD, mid, 'home', 2, 1, 6.0)
        assert second is None  # already settled — no work
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # not paid twice

    def test_settle_after_void_is_noop(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 2.0, 1.0, 1000)  # bal 900
        db.bet_void(GUILD, mid, 4.0)  # refunded → 1000
        assert db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1000  # not paid on top


class TestVoid:
    def test_refunds_and_cancels(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 2.0, 1.0, 1000)  # bal 700
        refunds = db.bet_void(GUILD, mid, 9.0)
        assert dict(refunds) == {USER_A: 300}
        assert db.bet_get_balance(GUILD, USER_A) == 1000  # fully restored
        assert db.bet_market_get(mid).status == 'cancelled'

    def test_voided_excluded_from_profit(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 2.0, 1.0, 1000)
        db.bet_void(GUILD, mid, 9.0)
        assert db.bet_profit_leaderboard(GUILD) == []

    def test_void_after_settle_is_noop(self, db):
        """A void must not refund on top of a payout already credited."""
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 2.0, 1.0, 1000)  # bal 900
        db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)  # paid 200 → 1100
        assert db.bet_void(GUILD, mid, 9.0) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # unchanged
        assert db.bet_market_get(mid).status == 'settled'  # not flipped


class TestMarketsOpen:
    def test_lists_only_open(self, db):
        open_mid = _make_market(db, commence=1000.0)
        settled = db.bet_market_create(
            GUILD, CH, 'evt2', 'soccer_epl', 'A', 'B', 2000.0,
            2.0, 3.0, 4.0, USER_A, 0.0)
        db.bet_settle(GUILD, settled, 'home', 1, 0, 3.0)
        rows = db.bet_markets_open(GUILD)
        assert [m.market_id for m in rows] == [open_mid]

    def test_guild_isolation(self, db):
        _make_market(db)
        assert db.bet_markets_open('999') == []


class TestBalanceLeaderboard:
    def test_orders_by_balance_desc(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_ensure_wallet(GUILD, USER_B, 1000)
        db.conn.execute('UPDATE bet_wallet SET balance = 1500 WHERE user_id = ?',
                        (USER_B,))
        db.conn.commit()
        rows = db.bet_balance_leaderboard(GUILD)
        assert [r.user_id for r in rows] == [USER_B, USER_A]


# ── Migration ──────────────────────────────────────────────────────────────

class TestMigration:
    def test_upgrade_creates_tables(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute(
            'INSERT INTO bet_wallet (guild_id, user_id, balance) VALUES (?, ?, ?)',
            ('1', '10', 1000))
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'A', 'B', 0.0, 2.0, 3.0, 4.0, '9', 0.0))
        # thread_id column exists
        conn.execute('UPDATE bet_market SET thread_id = ? WHERE event_id = ?',
                     ('77', 'e'))
        conn.execute(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, odds, placed_at) '
            'VALUES (?, ?, ?, ?, ?, ?)', (1, '10', 'home', 100, 2.0, 0.0))
        assert conn.execute('SELECT COUNT(*) FROM bet_wager').fetchone()[0] == 1
        conn.close()


# ── Cog bet-execution path ──────────────────────────────────────────────────

class TestExecuteBet:
    """_execute_bet wires parse_amount + escrow through the cog against a real
    DB, returning a status the command/listener render differently."""

    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        return Betting(bot=None)

    def _user(self, uid):
        class _U:
            id = uid
        return _U()

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_ok(self, db, cog):
        mid = _make_market(db, commence=1e12)  # far future → open
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '300'))
        assert status == 'ok'
        assert data['stake'] == 300 and data['odds'] == 2.0
        assert data['potential'] == 600 and data['balance'] == 700

    def test_closed_after_kickoff(self, db, cog):
        mid = _make_market(db, commence=1.0)  # already kicked off
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '100'))
        assert status == 'closed'

    def test_invalid_amount(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', 'lots'))
        assert status == 'invalid'

    def test_insufficient(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '5000'))
        assert status == 'insufficient' and data['balance'] == 1000

    def test_all_in_uses_full_balance(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'away', 'all'))
        assert status == 'ok' and data['stake'] == 1000 and data['balance'] == 0
