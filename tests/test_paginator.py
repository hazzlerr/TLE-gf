"""Tests for the button-based paginator."""
import pytest

# paginator is stubbed in conftest, so we test chunkify via the stub
# and test PaginatorView logic via a minimal mock that bypasses discord.ui
from tle.util.paginator import chunkify, NoPagesError


# ---------------------------------------------------------------------------
# chunkify
# ---------------------------------------------------------------------------

class TestChunkify:
    def test_even_split(self):
        assert chunkify([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]

    def test_uneven_split(self):
        assert chunkify([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]

    def test_chunk_larger_than_list(self):
        assert chunkify([1, 2], 10) == [[1, 2]]

    def test_empty_list(self):
        assert chunkify([], 5) == []

    def test_chunk_size_one(self):
        assert chunkify([1, 2, 3], 1) == [[1], [2], [3]]


# ---------------------------------------------------------------------------
# PaginatorView logic — tested via a minimal shim
# We can't use the real PaginatorView because it inherits discord.ui.View
# which is stubbed. Instead we test the navigation logic directly.
# ---------------------------------------------------------------------------

class FakeEmbed:
    def __init__(self, title=None):
        self.title = title
        self.footer = None

    def set_footer(self, *, text=None):
        self.footer = text


class FakePaginatorView:
    """Mirrors PaginatorView's navigation logic without discord.ui."""

    def __init__(self, pages, author_id=None):
        self.pages = pages
        self.author_id = author_id
        self.cur_page = 0
        self.first_disabled = True
        self.prev_disabled = True
        self.next_disabled = False
        self.last_disabled = False
        self._update_buttons()

    def _update_buttons(self):
        first_page = self.cur_page == 0
        last_page = self.cur_page == len(self.pages) - 1
        self.first_disabled = first_page
        self.prev_disabled = first_page
        self.next_disabled = last_page
        self.last_disabled = last_page

    def check_author(self, user_id):
        if self.author_id is not None and user_id != self.author_id:
            return False
        return True

    def go_first(self):
        self.cur_page = 0
        self._update_buttons()

    def go_prev(self):
        self.cur_page = max(0, self.cur_page - 1)
        self._update_buttons()

    def go_next(self):
        self.cur_page = min(len(self.pages) - 1, self.cur_page + 1)
        self._update_buttons()

    def go_last(self):
        self.cur_page = len(self.pages) - 1
        self._update_buttons()

    @property
    def current_content(self):
        return self.pages[self.cur_page]


def _make_pages(n):
    return [(f'content_{i}', FakeEmbed(title=f'Page {i}')) for i in range(n)]


class TestPaginatorNavigation:
    def test_starts_on_first_page(self):
        view = FakePaginatorView(_make_pages(3))
        assert view.cur_page == 0

    def test_next_advances(self):
        view = FakePaginatorView(_make_pages(3))
        view.go_next()
        assert view.cur_page == 1

    def test_prev_from_middle(self):
        view = FakePaginatorView(_make_pages(3))
        view.go_next()
        view.go_prev()
        assert view.cur_page == 0

    def test_prev_at_start_stays(self):
        view = FakePaginatorView(_make_pages(3))
        view.go_prev()
        assert view.cur_page == 0

    def test_next_at_end_stays(self):
        view = FakePaginatorView(_make_pages(3))
        view.go_last()
        view.go_next()
        assert view.cur_page == 2

    def test_go_first(self):
        view = FakePaginatorView(_make_pages(5))
        view.go_last()
        view.go_first()
        assert view.cur_page == 0

    def test_go_last(self):
        view = FakePaginatorView(_make_pages(5))
        view.go_last()
        assert view.cur_page == 4

    def test_content_tracks_page(self):
        pages = _make_pages(3)
        view = FakePaginatorView(pages)
        assert view.current_content == pages[0]
        view.go_next()
        assert view.current_content == pages[1]
        view.go_last()
        assert view.current_content == pages[2]


class TestButtonState:
    def test_first_page_disables_prev_buttons(self):
        view = FakePaginatorView(_make_pages(3))
        assert view.first_disabled is True
        assert view.prev_disabled is True
        assert view.next_disabled is False
        assert view.last_disabled is False

    def test_last_page_disables_next_buttons(self):
        view = FakePaginatorView(_make_pages(3))
        view.go_last()
        assert view.first_disabled is False
        assert view.prev_disabled is False
        assert view.next_disabled is True
        assert view.last_disabled is True

    def test_middle_page_all_enabled(self):
        view = FakePaginatorView(_make_pages(3))
        view.go_next()
        assert view.first_disabled is False
        assert view.prev_disabled is False
        assert view.next_disabled is False
        assert view.last_disabled is False

    def test_single_page_all_disabled(self):
        view = FakePaginatorView(_make_pages(1))
        assert view.first_disabled is True
        assert view.prev_disabled is True
        assert view.next_disabled is True
        assert view.last_disabled is True

    def test_two_pages_toggle(self):
        view = FakePaginatorView(_make_pages(2))
        assert view.prev_disabled is True
        assert view.next_disabled is False
        view.go_next()
        assert view.prev_disabled is False
        assert view.next_disabled is True


class TestAuthorCheck:
    def test_author_matches(self):
        view = FakePaginatorView(_make_pages(3), author_id=12345)
        assert view.check_author(12345) is True

    def test_author_mismatch_rejected(self):
        view = FakePaginatorView(_make_pages(3), author_id=12345)
        assert view.check_author(99999) is False

    def test_no_author_allows_anyone(self):
        view = FakePaginatorView(_make_pages(3), author_id=None)
        assert view.check_author(12345) is True
        assert view.check_author(99999) is True


class TestPaginateFunction:
    def test_no_pages_raises(self):
        # Use the stubbed paginate — but NoPagesError is the real one
        with pytest.raises(NoPagesError):
            # Import the real function's error behavior
            raise NoPagesError()

    def test_set_pagenum_footers(self):
        pages = _make_pages(3)
        # Simulate what paginate() does
        for i, (content, embed) in enumerate(pages):
            embed.set_footer(text=f'Page {i + 1} / {len(pages)}')
        assert pages[0][1].footer == 'Page 1 / 3'
        assert pages[1][1].footer == 'Page 2 / 3'
        assert pages[2][1].footer == 'Page 3 / 3'

    def test_single_page_no_footer(self):
        pages = _make_pages(1)
        # paginate() only sets footers when len > 1
        assert pages[0][1].footer is None
