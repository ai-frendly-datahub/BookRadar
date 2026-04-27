from __future__ import annotations

from bookradar.analyzer import apply_entity_rules
from bookradar.config_loader import load_category_config
from bookradar.models import Article, EntityDefinition


def _make_article(title: str, summary: str = "") -> Article:
    return Article(
        title=title,
        link=f"https://example.com/{hash(title)}",
        summary=summary,
        published=None,
        source="TestSource",
        category="test",
    )


class TestApplyEntityRules:
    """Unit tests for apply_entity_rules keyword matching."""

    def test_keyword_match(self):
        """Keyword in title triggers a match."""
        articles = [_make_article("Python release notes")]
        entities = [EntityDefinition(name="Python", display_name="Python", keywords=["python"])]

        result = apply_entity_rules(articles, entities)

        assert len(result) == 1
        assert "Python" in result[0].matched_entities
        assert "python" in result[0].matched_entities["Python"]

    def test_no_match(self):
        """No match when keyword is absent."""
        articles = [_make_article("Java release notes")]
        entities = [EntityDefinition(name="Python", display_name="Python", keywords=["python"])]

        result = apply_entity_rules(articles, entities)

        assert len(result) == 1
        assert result[0].matched_entities == {}

    def test_case_insensitive(self):
        """Matching is case-insensitive."""
        articles = [_make_article("PYTHON is Great")]
        entities = [EntityDefinition(name="Python", display_name="Python", keywords=["python"])]

        result = apply_entity_rules(articles, entities)

        assert "Python" in result[0].matched_entities

    def test_multiple_entities(self):
        """Multiple entities can match the same article."""
        articles = [_make_article("Python and Rust comparison")]
        entities = [
            EntityDefinition(name="Python", display_name="Python", keywords=["python"]),
            EntityDefinition(name="Rust", display_name="Rust", keywords=["rust"]),
        ]

        result = apply_entity_rules(articles, entities)

        assert "Python" in result[0].matched_entities
        assert "Rust" in result[0].matched_entities

    def test_empty_articles(self):
        """Empty article list returns empty result."""
        entities = [EntityDefinition(name="Python", display_name="Python", keywords=["python"])]

        result = apply_entity_rules([], entities)

        assert result == []

    def test_summary_match(self):
        """Keywords in summary also trigger matches."""
        articles = [_make_article("Release notes", summary="Updated python bindings")]
        entities = [EntityDefinition(name="Python", display_name="Python", keywords=["python"])]

        result = apply_entity_rules(articles, entities)

        assert "Python" in result[0].matched_entities

    def test_non_ascii_keyword(self):
        """Non-ASCII (Korean) keywords match via substring."""
        articles = [_make_article("신간 도서 출시 소식")]
        entities = [EntityDefinition(name="도서", display_name="도서", keywords=["도서"])]

        result = apply_entity_rules(articles, entities)

        assert "도서" in result[0].matched_entities

    def test_book_config_matches_book_news(self):
        """Book news headlines should be classified by the real category config."""
        category = load_category_config("book")
        articles = [_make_article("The Book News We Covered This Week")]

        result = apply_entity_rules(articles, category.entities)

        assert result[0].matched_entities["BookType"] == ["book news"]

    def test_book_config_does_not_match_pronoun_it_as_genre(self):
        """The book config must not classify English pronoun 'it' as the IT genre."""
        category = load_category_config("book")
        articles = [
            _make_article(
                "Too hot to handle? Why it's time for authors to rediscover sex",
                summary="A fiction critic argues that contemporary writers avoid sex.",
            )
        ]

        result = apply_entity_rules(articles, category.entities)

        assert result[0].matched_entities["Genre"] == ["fiction"]
        assert "it" not in result[0].matched_entities["Genre"]
