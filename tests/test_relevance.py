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


def test_apply_source_context_entities_replaces_stale_source_context_tags() -> None:
    article = Article(
        title="Weekly rankings",
        link="https://example.com/rank",
        summary="",
        published=None,
        source="알라딘 베스트셀러",
        category="book",
        matched_entities={"SourceSignal": ["editorial_coverage", "manual_review_flag"]},
    )
    source = Source(
        name="알라딘 베스트셀러",
        type="rss",
        url="https://example.com/feed",
        content_type="bestseller",
        trust_tier="T1_official",
        config={"event_model": "sales_ranking"},
    )

    classified = apply_source_context_entities([article], [source])

    assert classified[0].matched_entities["SourceSignal"] == [
        "manual_review_flag",
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


def test_relevance_passes_non_book_articles_and_drops_invalid_book_content() -> None:
    non_book = Article(
        title="Tech story",
        link="https://example.com/tech",
        summary="",
        published=None,
        source="Tech",
        category="tech",
    )
    invalid = Article(
        title="Page not found",
        link="https://example.com/404",
        summary="404",
        published=None,
        source="Books",
        category="book",
        matched_entities={"Genre": ["fiction"]},
    )
    source = Source(name="Books", type="rss", url="https://example.com/feed")

    assert apply_source_context_entities([non_book], [source]) == [non_book]
    assert filter_relevant_articles([non_book, invalid], [source]) == [non_book]


def test_filter_relevant_articles_keeps_404_inside_normal_identifiers() -> None:
    article = Article(
        title="New book release",
        link="https://example.com/books/404172",
        summary=(
            "ISBN:<span class=\"isbn13\">9791193540442</span> "
            "Cover https://image.example.com/product/404/cover.jpg "
            "Coverage from https://www.404media.co/book-scanning-story"
        ),
        published=None,
        source="Books",
        category="book",
        matched_entities={"Genre": ["fiction"]},
    )
    source = Source(name="Books", type="rss", url="https://example.com/feed")

    assert filter_relevant_articles([article], [source]) == [article]


def test_filter_relevant_articles_drops_http_error_content() -> None:
    article = Article(
        title="Feed fetch failed",
        link="https://example.com/books",
        summary="404 Client Error: Not Found for url: https://example.com/books",
        published=None,
        source="Books",
        category="book",
        matched_entities={"Genre": ["fiction"]},
    )
    source = Source(name="Books", type="rss", url="https://example.com/feed")

    assert filter_relevant_articles([article], [source]) == []


def test_source_context_tags_cover_operational_content_types() -> None:
    articles = [
        Article(
            title="Library update",
            link="https://example.com/library",
            summary="",
            published=None,
            source="Library",
            category="book",
        ),
        Article(
            title="Author event",
            link="https://example.com/event",
            summary="",
            published=None,
            source="Events",
            category="book",
        ),
        Article(
            title="Award",
            link="https://example.com/award",
            summary="",
            published=None,
            source="Awards",
            category="book",
        ),
        Article(
            title="Community",
            link="https://example.com/community",
            summary="",
            published=None,
            source="Community",
            category="book",
        ),
    ]
    sources = [
        Source(name="Library", type="rss", url="", content_type="library_lending"),
        Source(name="Events", type="rss", url="", content_type="author_event"),
        Source(name="Awards", type="rss", url="", content_type="award"),
        Source(name="Community", type="rss", url="", content_type="community"),
    ]

    classified = apply_source_context_entities(articles, sources)

    assert classified[0].matched_entities["SourceSignal"] == ["library_lending"]
    assert classified[1].matched_entities["SourceSignal"] == ["author_event"]
    assert classified[2].matched_entities["SourceSignal"] == ["award_signal"]
    assert classified[3].matched_entities["SourceSignal"] == ["community_book_signal"]
