"""Shared fakes and fixtures for the SubFilter tests.

Split out of ``test_subfilter`` so each test module stays under the project's
500-line limit. Import the builders you need plus ``_inject_cache`` (an autouse
fixture — importing it into a test module activates it there).
"""
import pytest

from tle.util import codeforces_api as cf


# ---------------------------------------------------------------------------
# Helpers to build fake CF objects
# ---------------------------------------------------------------------------

def _contest(cid, name='Codeforces Round 900 (Div. 2)'):
    return cf.Contest(id=cid, name=name)


def _problem(contest_id=1, index='A', name='Test', rating=1500, tags=None):
    return cf.Problem(
        contestId=contest_id, problemsetName=None, index=index,
        name=name, type='PROGRAMMING', points=None, rating=rating,
        tags=tags or [],
    )


def _party(handle='tourist', ptype='CONTESTANT', contest_id=1):
    return cf.Party(
        contestId=contest_id,
        members=[cf.Member(handle=handle)],
        participantType=ptype,
    )


def _sub(sid=1, contest_id=1, handle='tourist', ptype='CONTESTANT',
         rating=1500, tags=None, verdict='OK', created=1000, name='Test',
         index='A'):
    return cf.Submission(
        id=sid, contestId=contest_id,
        problem=_problem(contest_id, index=index, name=name,
                         rating=rating, tags=tags),
        author=_party(handle, ptype, contest_id),
        programmingLanguage='C++', verdict=verdict,
        creationTimeSeconds=created, relativeTimeSeconds=0,
    )


# ---------------------------------------------------------------------------
# Fake contest cache injected into codeforces_common.cache2
# ---------------------------------------------------------------------------

class _FakeContestCache:
    def __init__(self, contests):
        self.contest_by_id = {c.id: c for c in contests}

    def get_contest(self, cid):
        return self.contest_by_id.get(cid, _contest(cid, 'Unknown'))


class _FakeCache2:
    def __init__(self, contests):
        self.contest_cache = _FakeContestCache(contests)


@pytest.fixture(autouse=True)
def _inject_cache(monkeypatch):
    """Inject a fake cache2 with standard contests into codeforces_common."""
    import tle.util.codeforces_common as cf_common
    contests = [
        _contest(1, 'Codeforces Round 900 (Div. 2)'),
        _contest(2, 'Codeforces Round 901 (Div. 1)'),
        _contest(3, 'Educational Codeforces Round 160'),
        _contest(4, 'Codeforces Round 902 (Div. 1 + Div. 2)'),
        _contest(5, 'Codeforces Global Round 25'),
    ]
    monkeypatch.setattr(cf_common, 'cache2', _FakeCache2(contests))
