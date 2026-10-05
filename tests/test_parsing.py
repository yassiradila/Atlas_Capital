"""
Unit tests for data parsing utilities.
Run with: pytest tests/test_parsing.py
"""

import pytest
from atlas.scraping.parser import clean_slug, parse_french_date, parse_french_number


class TestCleanSlug:
    def test_basic_slug(self):
        assert clean_slug("Attijariwafa Bank") == "attijariwafa-bank"

    def test_slug_with_extra_spaces(self):
        assert clean_slug("  maroc   telecom  ") == "maroc-telecom"

    def test_empty_string(self):
        assert clean_slug("") == ""


class TestParseFrenchNumber:
    def test_decimal_comma(self):
        assert parse_french_number("123,45") == 123.45

    def test_thousands_space(self):
        assert parse_french_number("1 250,50") == 1250.50

    def test_non_breaking_space(self):
        # Non-breaking space \xa0 common in French web scraping
        assert parse_french_number("1\xa0250,50") == 1250.50

    def test_narrow_no_break_space(self):
        # Narrow no-break space \u202f
        assert parse_french_number("10\u202f000,00") == 10000.00

    def test_percentage_positive(self):
        assert parse_french_number("+1,25%") == 1.25

    def test_percentage_negative(self):
        assert parse_french_number("-0,75%") == -0.75

    def test_dash_and_empty(self):
        assert parse_french_number("-") is None
        assert parse_french_number("") is None
        assert parse_french_number(None) is None

    def test_invalid_string(self):
        assert parse_french_number("abc") is None


class TestParseFrenchDate:
    def test_slash_format(self):
        assert parse_french_date("15/05/2024") == "2024-05-15"

    def test_dash_format(self):
        assert parse_french_date("01-01-2023") == "2023-01-01"

    def test_already_iso(self):
        assert parse_french_date("2024-05-15") == "2024-05-15"

    def test_invalid_date(self):
        assert parse_french_date("not-a-date") is None
        assert parse_french_date(None) is None
