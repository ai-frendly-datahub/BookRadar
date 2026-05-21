from __future__ import annotations

import calendar
import html
import os
import re
import threading
import time
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from pybreaker import CircuitBreakerError
from radar_core import AdaptiveThrottler, CrawlHealthStore
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import NetworkError, ParseError, SourceError
from .models import Article, Source
from .resilience import get_circuit_breaker_manager


_DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (compatible; RadarTemplateBot/1.0; +https://github.com/zzragida/ai-frendly-datahub)",
}
_DEFAULT_HEALTH_DB_PATH = "data/radar_data.duckdb"
_COLLECTION_CONTROL_LOCK = threading.Lock()
_ACTIVE_THROTTLER: AdaptiveThrottler | None = None
_ACTIVE_HEALTH_STORE: CrawlHealthStore | None = None
_BESTSELLER_RANK_RE = re.compile(r"\[[^\]]*베스트셀러\s*(\d{1,5})위[^\]]*\]")
_ISBN_RE = re.compile(r"\b(?:97[89][-\s]?)?\d[-\s]?\d{2,5}[-\s]?\d{2,7}[-\s]?[\dX]\b")


def _set_collection_controls(throttler: AdaptiveThrottler, health_store: CrawlHealthStore) -> None:
    global _ACTIVE_THROTTLER, _ACTIVE_HEALTH_STORE
    with _COLLECTION_CONTROL_LOCK:
        _ACTIVE_THROTTLER = throttler
        _ACTIVE_HEALTH_STORE = health_store


def _clear_collection_controls() -> None:
    global _ACTIVE_THROTTLER, _ACTIVE_HEALTH_STORE
    with _COLLECTION_CONTROL_LOCK:
        _ACTIVE_THROTTLER = None
        _ACTIVE_HEALTH_STORE = None


def _get_collection_controls() -> tuple[AdaptiveThrottler | None, CrawlHealthStore | None]:
    with _COLLECTION_CONTROL_LOCK:
        return _ACTIVE_THROTTLER, _ACTIVE_HEALTH_STORE


class RateLimiter:
    def __init__(self, min_interval: float = 0.5):
        self._min_interval: float = min_interval
        self._last_request: float = 0.0
        self._lock: threading.Lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()


def _resolve_max_workers(max_workers: int | None = None) -> int:
    if max_workers is None:
        raw_value = os.environ.get("RADAR_MAX_WORKERS", "5")
        try:
            parsed = int(raw_value)
        except ValueError:
            parsed = 5
    else:
        parsed = max_workers

    return max(1, min(parsed, 10))


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_DEFAULT_HEADERS)

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def _fetch_url_with_retry(
    url: str,
    timeout: int,
    headers: dict[str, str] | None = None,
    session: requests.Session | None = None,
    source_name: str | None = None,
    throttler: AdaptiveThrottler | None = None,
    health_store: CrawlHealthStore | None = None,
    max_attempts: int = 3,
) -> requests.Response:
    """Fetch URL with retry logic on transient errors."""
    merged = {**_DEFAULT_HEADERS, **(headers or {})}
    if throttler is None or health_store is None:
        active_throttler, active_health_store = _get_collection_controls()
        throttler = throttler or active_throttler
        health_store = health_store or active_health_store

    retryable_errors = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    )

    for attempt in range(max_attempts):
        if source_name is not None and throttler is not None:
            throttler.acquire(source_name)

        try:
            if session is not None:
                response = session.get(url, timeout=timeout, headers=merged)
            else:
                response = requests.get(url, timeout=timeout, headers=merged)
            response.raise_for_status()

            if source_name is not None and throttler is not None:
                throttler.record_success(source_name)
                if health_store is not None:
                    delay = throttler.get_current_delay(source_name)
                    health_store.record_success(source_name, delay)

            return response
        except retryable_errors as exc:
            if source_name is not None and throttler is not None:
                retry_after: int | str | None = None
                if isinstance(exc, requests.exceptions.HTTPError):
                    response = exc.response
                    if response is not None and response.status_code == 429:
                        retry_after = _parse_retry_after(response.headers.get("Retry-After"))

                throttler.record_failure(source_name, retry_after=retry_after)
                if health_store is not None:
                    delay = throttler.get_current_delay(source_name)
                    health_store.record_failure(source_name, str(exc), delay)

            if attempt == max_attempts - 1:
                raise

    raise RuntimeError("Retry loop exited unexpectedly")


def _parse_retry_after(value: str | None) -> int | str | None:
    if value is None:
        return None

    stripped = value.strip()
    if not stripped:
        return None

    if stripped.isdigit():
        return int(stripped)

    return stripped


def _source_bool(source: Source, key: str) -> bool:
    value = source.config.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _collect_reddit_pass(
    sources: list[Source],
    *,
    category: str,
    limit_per_source: int,
    timeout: int,
    health_db_path: str | None,
) -> tuple[list[Article], list[str]]:
    from radar_core.reddit_collector import collect_reddit_sources

    return collect_reddit_sources(
        sources=sources,
        category=category,
        limit=limit_per_source,
        timeout=timeout,
        health_db_path=health_db_path,
    )


def _detect_encoding(response: requests.Response) -> str:
    """Detect encoding for Korean .kr sites that may use EUC-KR."""
    content_type = response.headers.get("Content-Type", "")
    if "euc-kr" in content_type.lower() or "euc_kr" in content_type.lower():
        return "euc-kr"
    if "charset=" in content_type.lower():
        for part in content_type.split(";"):
            part = part.strip().lower()
            if part.startswith("charset="):
                return part.split("=", 1)[1].strip()
    return "utf-8"


def collect_sources(
    sources: list[Source],
    *,
    category: str,
    limit_per_source: int = 30,
    timeout: int = 15,
    min_interval_per_host: float = 0.5,
    max_workers: int | None = None,
    health_db_path: str | None = None,
) -> tuple[list[Article], list[str]]:
    """Fetch items from all configured sources, returning articles and errors."""
    articles: list[Article] = []
    errors: list[str] = []
    enabled_sources = [source for source in sources if source.enabled]
    rss_sources = [source for source in enabled_sources if source.type.lower() == "rss"]
    reddit_sources = [source for source in enabled_sources if source.type.lower() == "reddit"]
    unsupported_sources = [
        source
        for source in enabled_sources
        if source.type.lower() not in {"rss", "reddit"}
    ]
    manager = get_circuit_breaker_manager()
    workers = _resolve_max_workers(max_workers)
    resolved_health_db_path = health_db_path or os.environ.get(
        "RADAR_CRAWL_HEALTH_DB_PATH", _DEFAULT_HEALTH_DB_PATH
    )
    source_hosts: dict[str, str] = {
        source.name: (urlparse(source.url).netloc.lower() or source.name)
        for source in rss_sources
    }
    rate_limiters: dict[str, RateLimiter] = {
        host: RateLimiter(min_interval=min_interval_per_host) for host in set(source_hosts.values())
    }
    throttler = AdaptiveThrottler(min_delay=max(0.001, min_interval_per_host))
    health_store = CrawlHealthStore(resolved_health_db_path)
    _set_collection_controls(throttler, health_store)

    def _collect_for_source(source: Source) -> tuple[list[Article], list[str]]:
        if (
            not _source_bool(source, "bypass_crawl_health")
            and health_store.is_disabled(source.name)
        ):
            return [], [f"{source.name}: Source disabled (crawl health threshold reached)"]

        host = source_hosts[source.name]
        rate_limiters[host].acquire()

        try:
            breaker = manager.get_breaker(source.name)
            source_session = _create_session()
            try:
                result = breaker.call(
                    _collect_single,
                    source,
                    category=category,
                    limit=limit_per_source,
                    timeout=timeout,
                    session=source_session,
                )
            finally:
                source_session.close()
            return result, []
        except CircuitBreakerError:
            return [], [f"{source.name}: Circuit breaker open (source unavailable)"]
        except SourceError as exc:
            return [], [str(exc)]
        except ParseError as exc:
            throttler.record_failure(source.name)
            health_store.record_failure(
                source.name,
                str(exc),
                throttler.get_current_delay(source.name),
            )
            return [], [f"{source.name}: {exc}"]
        except NetworkError as exc:
            return [], [f"{source.name}: {exc}"]
        except Exception as exc:
            return [], [f"{source.name}: Unexpected error - {type(exc).__name__}: {exc}"]

    try:
        if workers == 1:
            for source in rss_sources:
                source_articles, source_errors = _collect_for_source(source)
                articles.extend(source_articles)
                errors.extend(source_errors)
        else:
            if rss_sources:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_map: list[Future[tuple[list[Article], list[str]]]] = [
                        executor.submit(_collect_for_source, source) for source in rss_sources
                    ]

                    for future in future_map:
                        source_articles, source_errors = future.result()
                        articles.extend(source_articles)
                        errors.extend(source_errors)

        if reddit_sources:
            try:
                reddit_articles, reddit_errors = _collect_reddit_pass(
                    reddit_sources,
                    category=category,
                    limit_per_source=limit_per_source,
                    timeout=timeout,
                    health_db_path=resolved_health_db_path,
                )
                articles.extend(reddit_articles)
                errors.extend(reddit_errors)
            except ImportError:
                errors.append(
                    f"Reddit collection unavailable for {len(reddit_sources)} source(s). "
                    "Ensure radar-core reddit support is installed."
                )

        for source in unsupported_sources:
            errors.append(
                f"{source.name}: Source type '{source.type}' is cataloged but not collected by the book pipeline"
            )
    finally:
        health_store.close()
        _clear_collection_controls()

    return articles, errors


def _collect_single(
    source: Source,
    *,
    category: str,
    limit: int,
    timeout: int,
    session: requests.Session | None = None,
) -> list[Article]:
    if source.type.lower() != "rss":
        raise SourceError(source.name, f"Unsupported source type '{source.type}'")

    try:
        response = _fetch_url_with_retry(
            source.url,
            timeout,
            session=session,
            source_name=source.name,
        )
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        raise NetworkError(f"Network error fetching {source.name}: {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise SourceError(source.name, f"Request failed: {exc}", exc) from exc

    try:
        # Handle EUC-KR encoding for Korean .kr sites
        encoding = _detect_encoding(response)
        if encoding.lower().replace("-", "") == "euckr":
            content = response.content.decode("euc-kr", errors="replace").encode("utf-8")
        else:
            content = response.content

        if not content.strip():
            raise ParseError(f"Empty feed response from {source.name}")

        feed = feedparser.parse(content)
        if not feed.entries and not _source_bool(source, "allow_empty_feed"):
            raise ParseError(f"No feed entries found for {source.name}")
        items: list[Article] = []

        for entry in feed.entries:
            if len(items) >= limit:
                break

            published = _extract_datetime(entry)
            title = html.unescape(_entry_text(entry, "title").strip()) or "(no title)"
            summary = _entry_summary(entry, fallback=title)
            link = _entry_text(entry, "link").strip()
            if _is_sales_ranking_source(source):
                expanded = _expand_bestseller_entry(
                    source=source,
                    title=title,
                    link=link,
                    summary=summary,
                    published=published,
                    category=category,
                )
                if expanded:
                    items.extend(expanded[: max(0, limit - len(items))])
                    continue

            items.append(
                Article(
                    title=title,
                    link=link,
                    summary=summary,
                    published=published,
                    source=source.name,
                    category=category,
                )
            )

        return items
    except Exception as exc:
        raise ParseError(f"Failed to parse feed from {source.name}: {exc}") from exc


def _is_sales_ranking_source(source: Source) -> bool:
    content_type = source.content_type.strip().lower()
    if content_type in {"bestseller", "sales_ranking"}:
        return True
    event_model = source.config.get("event_model")
    return isinstance(event_model, str) and event_model.strip() == "sales_ranking"


def _expand_bestseller_entry(
    *,
    source: Source,
    title: str,
    link: str,
    summary: str,
    published: datetime | None,
    category: str,
) -> list[Article]:
    matches = list(_BESTSELLER_RANK_RE.finditer(summary))
    if not matches:
        return []

    articles: list[Article] = []
    for index, match in enumerate(matches):
        rank = int(match.group(1))
        end = matches[index + 1].start() if index + 1 < len(matches) else len(summary)
        chunk = summary[match.end() : end]
        soup = BeautifulSoup(chunk, "html.parser")
        book_title = _bestseller_title(soup)
        if not book_title:
            continue

        item_link = _ranked_event_link(
            item_link=_bestseller_link(soup),
            weekly_link=link,
            rank=rank,
            published=published,
        )
        isbn = _bestseller_isbn(chunk, soup)
        plain_summary = " ".join(soup.get_text(" ", strip=True).split())
        if isbn and "ISBN" not in plain_summary.upper():
            plain_summary = f"ISBN: {isbn}. {plain_summary}"

        articles.append(
            Article(
                title=f"{rank}위 {book_title}",
                link=item_link,
                summary=html.unescape(plain_summary),
                published=published,
                source=source.name,
                category=category,
            )
        )

    if articles:
        return articles

    return [
        Article(
            title=title,
            link=link,
            summary=html.unescape(summary.strip()),
            published=published,
            source=source.name,
            category=category,
        )
    ]


def _bestseller_title(soup: BeautifulSoup) -> str:
    title_node = soup.select_one("h2 a") or soup.select_one("h2")
    if title_node is None:
        return ""
    return html.unescape(title_node.get_text(" ", strip=True)).strip()


def _bestseller_link(soup: BeautifulSoup) -> str:
    link_node = soup.select_one("h2 a[href]")
    if link_node is None:
        return ""
    href = link_node.get("href")
    return str(href).strip() if href else ""


def _bestseller_isbn(chunk: str, soup: BeautifulSoup) -> str:
    isbn_node = soup.select_one(".isbn13")
    if isbn_node is not None:
        value = isbn_node.get_text("", strip=True)
        if value:
            return re.sub(r"[-\s]", "", value)

    match = _ISBN_RE.search(chunk)
    return re.sub(r"[-\s]", "", match.group(0)) if match else ""


def _ranked_event_link(
    *,
    item_link: str,
    weekly_link: str,
    rank: int,
    published: datetime | None,
) -> str:
    base = item_link.strip() or weekly_link.strip() or "about:blank"
    ranking_date = (
        published.astimezone(UTC).strftime("%Y%m%d%H%M%S")
        if published and published.tzinfo
        else (published.strftime("%Y%m%d%H%M%S") if published else "undated")
    )
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}radar_rank={rank}&radar_ranking_date={ranking_date}"


def _extract_datetime(entry: Mapping[str, Any]) -> datetime | None:
    """Parse a feed entry date into a timezone-aware datetime."""
    published_parsed = entry.get("published_parsed")
    if isinstance(published_parsed, time.struct_time):
        return datetime.fromtimestamp(calendar.timegm(published_parsed), tz=UTC)

    updated_parsed = entry.get("updated_parsed")
    if isinstance(updated_parsed, time.struct_time):
        return datetime.fromtimestamp(calendar.timegm(updated_parsed), tz=UTC)

    for key in ("published", "updated", "date"):
        raw = entry.get(key)
        if raw:
            try:
                dt = parsedate_to_datetime(str(raw))
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt
            except Exception:
                continue
    return None


def _entry_text(entry: Mapping[str, Any], key: str) -> str:
    value = entry.get(key)
    return value if isinstance(value, str) else ""


def _entry_summary(entry: Mapping[str, Any], *, fallback: str) -> str:
    summary = _entry_text(entry, "summary") or _entry_text(entry, "description")
    if not summary:
        _content = entry.get("content", [])
        if isinstance(_content, list) and _content:
            first_item = _content[0]
            if isinstance(first_item, Mapping):
                value = first_item.get("value")
                if isinstance(value, str):
                    summary = value

    summary = html.unescape(summary.strip())
    return summary or fallback
