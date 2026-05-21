from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import duckdb
import pytest
import requests
from radar_core import CrawlHealthStore

from bookradar.collector import (
    _collect_single,
    _detect_encoding,
    _expand_bestseller_entry,
    _extract_datetime,
    _fetch_url_with_retry,
    _parse_retry_after,
    _resolve_max_workers,
    _source_bool,
    collect_sources,
)
from bookradar.exceptions import ParseError, SourceError
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


def test_collect_sources_reports_disabled_and_reddit_import_errors(tmp_path) -> None:
    health_db = tmp_path / "health.duckdb"
    with CrawlHealthStore(str(health_db), failure_threshold=1) as store:
        store.record_failure("rss", error="previous outage", delay=1.0)

    sources = [
        Source(name="rss", type="rss", url="https://example.com/feed"),
        Source(name="reddit", type="reddit", url="https://www.reddit.com/r/books/"),
    ]

    with patch("bookradar.collector._collect_reddit_pass", side_effect=ImportError):
        articles, errors = collect_sources(
            sources,
            category="book",
            min_interval_per_host=0.0,
            max_workers=1,
            health_db_path=str(health_db),
        )

    assert articles == []
    assert any("Source disabled" in error for error in errors)
    assert any("Reddit collection unavailable" in error for error in errors)


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        (SourceError("rss", "bad source"), "[rss] bad source"),
        (ValueError("boom"), "Unexpected error - ValueError: boom"),
    ],
)
def test_collect_sources_reports_source_errors(tmp_path, exception, expected) -> None:
    source = Source(name="rss", type="rss", url="https://example.com/feed")

    with (
        patch("bookradar.collector._collect_single", side_effect=exception),
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
            health_db_path=str(tmp_path / "health.duckdb"),
        )

    assert articles == []
    assert any(expected in error for error in errors)


def test_collect_sources_records_parse_errors_in_crawl_health(tmp_path) -> None:
    health_db = tmp_path / "health.duckdb"
    source = Source(name="empty", type="rss", url="https://example.com/empty.xml")

    with (
        patch("bookradar.collector._collect_single", side_effect=ParseError("empty feed")),
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

    assert articles == []
    assert any("empty feed" in error for error in errors)
    with duckdb.connect(str(health_db), read_only=True) as con:
        row = con.execute(
            """
            SELECT failure_count, last_error
            FROM crawl_health
            WHERE source_name = 'empty'
            """
        ).fetchone()
    assert row == (1, "empty feed")


def test_collect_single_expands_aladin_bestseller_items() -> None:
    source = Source(
        name="알라딘 베스트셀러",
        type="rss",
        url="https://example.com/bestseller.xml",
        content_type="bestseller",
        config={"event_model": "sales_ranking"},
    )
    feed = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>[알라딘 베스트 RSS] 도서종합 분야 주간 베스트셀러</title>
      <link>https://example.com/weekly</link>
      <pubDate>Wed, 20 May 2026 08:00:00 GMT</pubDate>
      <description><![CDATA[
        <span>[주간 베스트셀러 1위]</span>
        <table><tr><td><h2><a href="https://example.com/book-1">프로젝트 헤일메리</a></h2>
        앤디 위어 / 알에이치코리아 / ISBN:<span class="isbn13">9788925588735</span></td></tr></table>
        <span>[주간 베스트셀러 2위]</span>
        <table><tr><td><h2><a href="https://example.com/book-2">포켓몬 생태도감</a></h2>
        주식회사 포켓몬 / 대원씨아이 / ISBN:<span class="isbn13">9791142350283</span></td></tr></table>
      ]]></description>
    </item>
  </channel>
</rss>
"""
    response = Mock()
    response.headers = {"Content-Type": "application/rss+xml; charset=utf-8"}
    response.content = feed.encode("utf-8")

    with patch("bookradar.collector._fetch_url_with_retry", return_value=response):
        articles = _collect_single(source, category="book", limit=10, timeout=1)

    assert [article.title for article in articles] == [
        "1위 프로젝트 헤일메리",
        "2위 포켓몬 생태도감",
    ]
    assert [article.link for article in articles] == [
        "https://example.com/book-1?radar_rank=1&radar_ranking_date=20260520080000",
        "https://example.com/book-2?radar_rank=2&radar_ranking_date=20260520080000",
    ]
    assert "9788925588735" in articles[0].summary


def test_collect_single_uses_title_when_feed_item_has_no_summary() -> None:
    source = Source(
        name="Publishers Weekly",
        type="rss",
        url="https://example.com/pw.xml",
        content_type="news",
    )
    feed = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Dharma Publishing Relaunches</title>
      <link>https://example.com/article</link>
      <pubDate>Wed, 20 May 2026 08:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""
    response = Mock()
    response.headers = {"Content-Type": "application/rss+xml; charset=utf-8"}
    response.content = feed.encode("utf-8")

    with patch("bookradar.collector._fetch_url_with_retry", return_value=response):
        articles = _collect_single(source, category="book", limit=10, timeout=1)

    assert articles[0].title == "Dharma Publishing Relaunches"
    assert articles[0].summary == "Dharma Publishing Relaunches"


def test_collect_single_rejects_empty_feed_response() -> None:
    source = Source(
        name="Shelf Awareness",
        type="rss",
        url="https://example.com/empty.xml",
        content_type="news",
    )
    response = Mock()
    response.headers = {"Content-Type": "application/rss+xml; charset=utf-8"}
    response.content = b""

    with patch("bookradar.collector._fetch_url_with_retry", return_value=response):
        with pytest.raises(ParseError, match="Empty feed response"):
            _collect_single(source, category="book", limit=10, timeout=1)


def test_extract_datetime_treats_struct_time_as_utc(monkeypatch) -> None:
    old_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "Asia/Seoul")
    if hasattr(time, "tzset"):
        time.tzset()

    try:
        parsed = _extract_datetime(
            {"published_parsed": time.struct_time((2026, 5, 20, 8, 0, 0, 2, 140, 0))}
        )
    finally:
        if old_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", old_tz)
        if hasattr(time, "tzset"):
            time.tzset()

    assert parsed == datetime(2026, 5, 20, 8, 0, tzinfo=UTC)


def test_fetch_url_records_success_and_retry_after_failure() -> None:
    response = Mock()
    response.raise_for_status.return_value = None
    session = Mock()
    session.get.return_value = response
    throttler = Mock()
    throttler.get_current_delay.return_value = 0.5
    health_store = Mock()

    result = _fetch_url_with_retry(
        "https://example.com/feed",
        3,
        headers={"X-Test": "1"},
        session=session,
        source_name="rss",
        throttler=throttler,
        health_store=health_store,
        max_attempts=1,
    )

    assert result is response
    assert session.get.call_args.kwargs["headers"]["X-Test"] == "1"
    throttler.record_success.assert_called_once_with("rss")
    health_store.record_success.assert_called_once_with("rss", 0.5)

    error_response = Mock()
    error_response.status_code = 429
    error_response.headers = {"Retry-After": "3"}
    http_error = requests.exceptions.HTTPError(response=error_response)
    error_session = Mock()
    error_session.get.return_value.raise_for_status.side_effect = http_error
    throttler = Mock()
    throttler.get_current_delay.return_value = 2.0
    health_store = Mock()

    with pytest.raises(requests.exceptions.HTTPError):
        _fetch_url_with_retry(
            "https://example.com/feed",
            3,
            session=error_session,
            source_name="rss",
            throttler=throttler,
            health_store=health_store,
            max_attempts=1,
        )

    throttler.record_failure.assert_called_once_with("rss", retry_after=3)
    health_store.record_failure.assert_called_once()


def test_collector_small_helpers(monkeypatch) -> None:
    monkeypatch.setenv("RADAR_MAX_WORKERS", "bad")
    assert _resolve_max_workers() == 5
    assert _resolve_max_workers(99) == 10
    assert _resolve_max_workers(0) == 1
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("  ") is None
    assert _parse_retry_after("17") == 17
    assert _parse_retry_after("Wed, 20 May 2026 08:00:00 GMT") == (
        "Wed, 20 May 2026 08:00:00 GMT"
    )
    assert _source_bool(Source(name="s", type="rss", url="", config={"x": "YES"}), "x") is True
    assert _source_bool(Source(name="s", type="rss", url="", config={"x": "no"}), "x") is False

    response = Mock()
    response.headers = {"Content-Type": "text/xml; charset=euc-kr"}
    assert _detect_encoding(response) == "euc-kr"
    response.headers = {"Content-Type": "text/xml; charset=Shift_JIS"}
    assert _detect_encoding(response) == "shift_jis"
    response.headers = {}
    assert _detect_encoding(response) == "utf-8"


def test_expand_bestseller_entry_fallbacks() -> None:
    source = Source(name="rank", type="rss", url="", content_type="bestseller")

    assert (
        _expand_bestseller_entry(
            source=source,
            title="weekly",
            link="https://example.com/weekly",
            summary="no ranks",
            published=None,
            category="book",
        )
        == []
    )

    articles = _expand_bestseller_entry(
        source=source,
        title="weekly",
        link="https://example.com/weekly?rss=1",
        summary="[주간 베스트셀러 1위]<table><tr><td>no title</td></tr></table>",
        published=None,
        category="book",
    )

    assert len(articles) == 1
    assert articles[0].title == "weekly"
    assert articles[0].link == "https://example.com/weekly?rss=1"

    ranked = _expand_bestseller_entry(
        source=source,
        title="weekly",
        link="https://example.com/weekly?rss=1",
        summary=(
            "[주간 베스트셀러 1위]"
            '<table><tr><td><h2><a href="https://example.com/book">Book</a></h2>'
            'ISBN:<span class="isbn13">9788925588735</span></td></tr></table>'
        ),
        published=datetime(2026, 5, 20, 8, 0, tzinfo=UTC),
        category="book",
    )

    assert ranked[0].link == (
        "https://example.com/book?radar_rank=1&radar_ranking_date=20260520080000"
    )
