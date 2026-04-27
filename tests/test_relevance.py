from __future__ import annotations

from bookradar.models import Article, Source
from bookradar.relevance import apply_source_context_entities, filter_relevant_articles


def test_apply_source_context_entities_tags_bestseller_source() -> None:
    article = Article(
        title="Weekly rankings",
        link="https://example.com/rank",
        summary="",
        published=None,
        source="YES24 베스트셀러",
        category="book",
        matched_entities={},
    )
    source = Source(
        name="YES24 베스트셀러",
        type="rss",
        url="https://example.com/feed",
        content_type="bestseller",
        trust_tier="T1_official",
    )

    classified = apply_source_context_entities([article], [source])

    assert classified[0].matched_entities["SourceSignal"] == [
        "official_book_source",
        "sales_ranking",
    ]


def test_filter_relevant_articles_keeps_book_review_source_without_keyword_match() -> None:
    article = Article(
        title="Books our editors loved",
        link="https://example.com/books",
        summary="Reading recommendations.",
        published=None,
        source="New York Times Books",
        category="book",
        matched_entities={},
    )
    source = Source(
        name="New York Times Books",
        type="rss",
        url="https://example.com/feed",
        content_type="review",
    )

    classified = apply_source_context_entities([article], [source])

    assert filter_relevant_articles(classified, [source]) == classified


def test_filter_relevant_articles_drops_unconfigured_source() -> None:
    article = Article(
        title="Old source row",
        link="https://example.com/old",
        summary="",
        published=None,
        source="Old Book Source",
        category="book",
        matched_entities={"Genre": ["fiction"]},
    )

    assert filter_relevant_articles([article], []) == []
