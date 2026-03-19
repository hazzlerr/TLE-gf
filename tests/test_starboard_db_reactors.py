"""Tests for starboard reactor DB methods."""
import pytest

from tests.test_starboard_db import FakeUserDb, GUILD, STAR, FIRE, THUMBS_UP


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


# =====================================================================
# Reactor tracking
# =====================================================================

class TestReactorCrud:
    def test_add_and_get_reactors(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        reactors = db.get_reactors('msg1', STAR)
        assert set(reactors) == {'user1', 'user2'}

    def test_add_reactor_idempotent(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user1')  # Duplicate
        assert db.get_reactor_count('msg1', STAR) == 1

    def test_remove_reactor(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        rc = db.remove_reactor('msg1', STAR, 'user1')
        assert rc == 1
        assert db.get_reactors('msg1', STAR) == ['user2']

    def test_remove_nonexistent_reactor(self, db):
        rc = db.remove_reactor('msg1', STAR, 'user1')
        assert rc == 0

    def test_get_reactor_count(self, db):
        assert db.get_reactor_count('msg1', STAR) == 0
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', STAR, 'user3')
        assert db.get_reactor_count('msg1', STAR) == 3

    def test_reactors_per_emoji_independent(self, db):
        """Same message, different emojis — reactors are independent."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', FIRE, 'user1')
        assert db.get_reactor_count('msg1', STAR) == 2
        assert db.get_reactor_count('msg1', FIRE) == 1

    def test_bulk_add_reactors(self, db):
        db.bulk_add_reactors('msg1', STAR, ['user1', 'user2', 'user3'])
        assert db.get_reactor_count('msg1', STAR) == 3
        assert set(db.get_reactors('msg1', STAR)) == {'user1', 'user2', 'user3'}

    def test_bulk_add_idempotent(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.bulk_add_reactors('msg1', STAR, ['user1', 'user2', 'user3'])
        assert db.get_reactor_count('msg1', STAR) == 3

    def test_bulk_add_empty_list(self, db):
        db.bulk_add_reactors('msg1', STAR, [])
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_int_user_ids(self, db):
        """Discord user IDs are ints — should be cast to str."""
        db.add_reactor('msg1', STAR, 123456789)
        assert db.get_reactors('msg1', STAR) == ['123456789']
        rc = db.remove_reactor('msg1', STAR, 123456789)
        assert rc == 1


class TestMergedReactorCount:
    def test_same_user_two_emojis_counts_once(self, db):
        """User reacted with both star and fire — merged count = 1."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')
        assert db.get_merged_reactor_count('msg1', [STAR, FIRE]) == 1

    def test_different_users_two_emojis(self, db):
        """user1 starred, user2 fired — merged count = 2."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user2')
        assert db.get_merged_reactor_count('msg1', [STAR, FIRE]) == 2

    def test_overlapping_users(self, db):
        """user1 did both, user2 only star, user3 only fire — merged = 3."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', FIRE, 'user3')
        assert db.get_merged_reactor_count('msg1', [STAR, FIRE]) == 3

    def test_single_emoji_matches_get_reactor_count(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        assert db.get_merged_reactor_count('msg1', [STAR]) == 2
        assert db.get_reactor_count('msg1', STAR) == 2

    def test_empty_emojis_returns_zero(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        assert db.get_merged_reactor_count('msg1', []) == 0


class TestReactorCascadeDelete:
    def test_remove_message_by_emoji_cleans_reactors(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')

        db.remove_starboard_message(original_msg_id='msg1', emoji=STAR)
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_remove_message_by_starboard_msg_id_cleans_reactors(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')

        db.remove_starboard_message(starboard_msg_id='sb1')
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_remove_message_all_emojis_cleans_reactors(self, db):
        db.add_starboard_message_v1('msg1', 'sb_s', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_f', GUILD, FIRE, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user2')

        db.remove_starboard_message(original_msg_id='msg1')
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg1', FIRE) == 0

    def test_remove_emoji_cleans_reactors(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg2', STAR, 'user2')

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg2', STAR) == 0

    def test_remove_emoji_preserves_other_emoji_reactors(self, db):
        """Removing star shouldn't touch fire reactors."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 3, 0xff0000)
        db.add_starboard_message_v1('msg1', 'sb_s', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_f', GUILD, FIRE, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg1', FIRE) == 1  # Preserved


class TestReplaceReactors:
    def test_replace_clears_old_and_inserts_new(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', STAR, 'ghost')  # Will be purged

        db.replace_reactors('msg1', [STAR], [(STAR, 'user1'), (STAR, 'user3')])
        assert set(db.get_reactors('msg1', STAR)) == {'user1', 'user3'}
        assert db.get_reactor_count('msg1', STAR) == 2

    def test_replace_with_empty_clears_all(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.replace_reactors('msg1', [STAR], [])
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_replace_multiple_emojis(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user2')
        db.add_reactor('msg1', FIRE, 'ghost')

        db.replace_reactors('msg1', [STAR, FIRE],
                            [(STAR, 'user1'), (FIRE, 'user2'), (FIRE, 'user3')])
        assert db.get_reactors('msg1', STAR) == ['user1']
        assert set(db.get_reactors('msg1', FIRE)) == {'user2', 'user3'}

    def test_replace_preserves_other_messages(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg2', STAR, 'user2')

        db.replace_reactors('msg1', [STAR], [])
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg2', STAR) == 1  # Untouched


class TestMergedCountWithAliases:
    """Test that get_merged_reactor_count correctly deduplicates across aliases."""

    def test_user_reacted_main_and_alias_counts_once(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')
        family = db.get_emoji_family(GUILD, STAR)
        assert db.get_merged_reactor_count('msg1', family) == 1

    def test_different_users_different_emojis(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user2')
        family = db.get_emoji_family(GUILD, STAR)
        assert db.get_merged_reactor_count('msg1', family) == 2

    def test_mixed_overlap(self, db):
        """user1 uses both, user2 uses only alias, user3 uses only main -> 3."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user2')
        db.add_reactor('msg1', STAR, 'user3')
        family = db.get_emoji_family(GUILD, STAR)
        assert db.get_merged_reactor_count('msg1', family) == 3

    def test_only_alias_reactions_count(self, db):
        """Even if nobody used the main emoji, alias reactions still count."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_reactor('msg1', THUMBS_UP, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user2')
        family = db.get_emoji_family(GUILD, STAR)
        assert db.get_merged_reactor_count('msg1', family) == 2

    def test_multiple_aliases(self, db):
        """Three aliases plus main, various overlaps."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD, FIRE, STAR)
        # user1 uses all three -> count 1
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')
        # user2 uses only fire -> count 1
        db.add_reactor('msg1', FIRE, 'user2')
        family = db.get_emoji_family(GUILD, STAR)
        assert db.get_merged_reactor_count('msg1', family) == 2
