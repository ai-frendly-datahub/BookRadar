from __future__ import annotations

from unittest.mock import Mock, patch

from radar_core import CrawlHealthStore

from bookradar.collector import collect_sources
from bookradar.models import Article, Source


def _pass_through_manager() -> Mock:
    breaker = Mock()
    breaker.call.side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
    manager = Mock()
    manager.get_breaker.return_value = breaker
    return manager


def test_collect_sources_routes_reddit_and_skips_disabled_sources(tmp_path) -> None:
    sources = [
        Source(name="rss", type="rss", url="https://example.com/feed"),
        Source(name="reddit", type="reddit", url="https://www.reddit.com/r/books/"),
        Source(name="catalog", type="mcp", url="https://example.com/mcp"),
        Source(
            name="disabled",
            type="rss",
            url="https://disabled.example.com/feed",
            enabled=False,
        ),
    ]
    rss_article = Article(
        title="rss-article",
        link="https://example.com/rss-article",
        summary="rss",
        published=None,
        source="rss",
        category="book",
    )
    reddit_article = Article(
        title="reddit-article",
        link="https://example.com/reddit-article",
        summary="reddit",
        published=None,
        source="reddit",
        category="book",
    )

    with (
        patch("bookradar.collector._collect_single", return_value=[rss_article]) as mock_rss,
        patch(
            "bookradar.collector._collect_reddit_pass",
            return_value=([reddit_article], []),
        ) as mock_reddit,
        patch(
            "bookradar.collector.get_circuit_breaker_manager",
            return_value=_pass_through_manager(),
        ),
    ):
        articles, errors = collect_sources(
            sources,
            category="book",
            min_interval_per_host=0.0,
            max_workers=1,
            health_db_path=str(tmp_path / "health.duckdb"),
        )

    assert [article.source for article in articles] == ["rss", "reddit"]
    assert mock_rss.call_count == 1
    assert mock_reddit.call_count == 1
    assert all("disabled" not in error for error in errors)
    assert any("cataloged but not collected" in error for error in errors)


def test_collect_sources_can_bypass_crawl_health(tmp_path) -> None:
    health_db = tmp_path / "health.duckdb"
    with CrawlHealthStore(str(health_db), failure_threshold=1) as store:
        store.record_failure("rss", error="previous outage", delay=1.0)

    source = Source(
        name="rss",
        type="rss",
        url="https://example.com/feed",
        config={"bypass_crawl_health": True},
    )
    article = Article(
        title="rss-article",
        link="https://example.com/rss-article",
        summary="rss",
        published=None,
        source="rss",
        category="book",
    )

    with (
        patch("bookradar.collector._collect_single", return_value=[article]) as mock_rss,
        patch(
            "bookradar.collector.get_circuit_breaker_manager",
            return_value=_pass_through_manager(),
        ),
    ):
        articles, errors = collect_sources(
            [source],
            category="book",
            min_interval_per_host=0.0,
            max_workers=1,
            health_db_path=str(health_db),
        )

    assert articles == [article]
    assert errors == []
    assert mock_rss.call_count == 1
