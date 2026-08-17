"""Focused tests for the pure counting-message parser."""

import pytest

from tle.cogs._counting_parser import (
    CORRECT,
    IGNORED,
    INVALID_FORMAT,
    WRONG_NUMBER,
    classify_count_attempt,
    could_be_count_attempt,
)


class TestCouldBeCountAttempt:
    @pytest.mark.parametrize('content', [
        '1', '73.7', '+9', '-9', '(11)', '0b102', '0xzz', '1G',
        'A', 'face', 'ABC',
    ])
    def test_keeps_numeric_shapes_and_possible_letter_hex(self, content):
        assert could_be_count_attempt(content) is True

    @pytest.mark.parametrize('content', [
        None, '', '   ', 'hello', 'nine', 'go 9', '9 then 10', 'A word',
    ])
    def test_filters_ordinary_or_multi_token_prose(self, content):
        assert could_be_count_attempt(content) is False


class TestCorrectCounts:
    @pytest.mark.parametrize(('content', 'expected', 'radix'), [
        ('9', 9, 10),
        ('1001', 9, 2),
        ('0b1001', 9, 2),
        ('0B1001', 9, 2),
        ('a', 10, 16),
        ('A', 10, 16),
        ('0xa', 10, 16),
        ('0XA', 10, 16),
        ('16', 16, 10),
        ('10000', 16, 2),
        ('10', 16, 16),
        ('dead', 0xDEAD, 16),
        ('1', 1, 10),
        ('  9  ', 9, 10),
    ])
    def test_accepts_canonical_decimal_binary_and_hex(
            self, content, expected, radix):
        result = classify_count_attempt(content, expected)
        assert result.status == CORRECT
        assert result.is_correct is True
        assert result.is_bad_attempt is False
        assert result.reason is None
        assert result.radix == radix
        assert result.value == expected

    @pytest.mark.parametrize(('content', 'expected', 'radix'), [
        ('10', 2, 2),
        ('10', 10, 10),
        ('10', 16, 16),
        ('11', 3, 2),
        ('11', 11, 10),
        ('11', 17, 16),
    ])
    def test_expected_value_disambiguates_bare_digits(
            self, content, expected, radix):
        assert classify_count_attempt(content, expected).radix == radix


class TestBadAttempts:
    @pytest.mark.parametrize('content', [
        '11', '73', '100000', 'babe1', 'abc123',
    ])
    def test_canonical_other_numbers_are_wrong_number(self, content):
        result = classify_count_attempt(content, expected=9)
        assert result.status == WRONG_NUMBER
        assert result.is_bad_attempt is True
        assert result.reason == WRONG_NUMBER

    @pytest.mark.parametrize(('content', 'radix', 'value'), [
        ('0b11', 2, 3),
        ('0x11', 16, 17),
    ])
    def test_prefixed_wrong_number_retains_unambiguous_radix(
            self, content, radix, value):
        result = classify_count_attempt(content, expected=9)
        assert result.status == WRONG_NUMBER
        assert (result.radix, result.value) == (radix, value)

    @pytest.mark.parametrize('content', [
        '73.7', '.9', '9.',
        '09', '001001', '0b01001', '0x09',
        '0b102', '0b', '0xgg', '0x',
    ])
    def test_malformed_or_noncanonical_tokens_are_invalid_format(self, content):
        result = classify_count_attempt(content, expected=9)
        assert result.status == INVALID_FORMAT
        assert result.is_bad_attempt is True
        assert result.reason == INVALID_FORMAT


class TestIgnoredMessages:
    @pytest.mark.parametrize('content', [
        None, '', 'hello', 'the answer is 9', '9 then 10', '9\n10',
        'face', 'b', 'abc', 'version2', 'hello2', 'user123',
        '2026-08-17', '123.png', '2nd', '42%', '1.2.3', '1/2',
        '9!', '(9)', '+9', '-9', '1g',
    ])
    def test_ordinary_prose_and_wrong_letter_only_hex_are_ignored(self, content):
        result = classify_count_attempt(content, expected=10)
        assert result.status == IGNORED
        assert result.is_correct is False
        assert result.is_bad_attempt is False
        assert result.reason is None

    def test_letter_only_hex_is_accepted_when_it_is_expected(self):
        result = classify_count_attempt('b', expected=11)
        assert (result.status, result.radix, result.value) == (CORRECT, 16, 11)


class TestExpectedValidation:
    @pytest.mark.parametrize('expected', [None, '9', 9.0, True])
    def test_expected_must_be_an_integer(self, expected):
        with pytest.raises(TypeError):
            classify_count_attempt('9', expected)

    def test_expected_must_be_non_negative(self):
        with pytest.raises(ValueError):
            classify_count_attempt('9', -1)
