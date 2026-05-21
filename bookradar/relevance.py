from __future__ import annotations

import re
from collections.abc import Iterable

from .models import Article, Source


BOOK_ENTITY_NAMES = {"Award", "Author", "BookEvent", "BookType", "Genre", "Publisher"}
SOURCE_CONTEXT_TAGS = {
    "author_event",
    "award_signal",
    "book_review_source",
    "community_book_signal",
    "editorial_coverage",
    "library_lending",
    "official_book_source",
    "publishing_news_source",
    "sales_ranking",
}
INVALID_CONTENT_RE = re.compile(
    r"\b(?:40[034]|50[0234])\b.{0,80}\b(?:client error|server error|not found|forbidden|service unavailable)\b"
    r"|\b(?:access denied|page not found|service unavailable)\b",
    re.IGNORECASE | re.DOTALL,
)


def apply_source_context_entities(
    articles: Iterable[Article],
    sources: Iterable[Source],
) -> list[Article]:
    source_map = {source.name: source for source in sources if source.enabled}
    classified: list[Article] = []
    for article in articles:
        if article.category != "book":
            classified.append(article)
            continue

        source = source_map.get(article.source)
        if source is None:
            continue

        tags = _source_context_tags(source)
        if tags:
            existing = article.matched_entities.get("SourceSignal", [])
            existing_values = existing if isinstance(existing, list) else [existing]
            preserved_values = {
                str(value)
                for value in existing_values
                if str(value) not in SOURCE_CONTEXT_TAGS
            }
            article.matched_entities["SourceSignal"] = sorted(
                preserved_values | set(tags)
            )
        classified.append(article)
    return classified


def filter_relevant_articles(
    articles: Iterable[Article],
    sources: Iterable[Source],
) -> list[Article]:
    source_map = {source.name: source for source in sources if source.enabled}
    filtered: list[Article] = []
    for article in articles:
        if article.category != "book":
            filtered.append(article)
            continue

        source = source_map.get(article.source)
        if source is None or _is_invalid(article):
            continue
        if _has_book_context(source) or _has_book_entity(article):
            filtered.append(article)
    return filtered


def _source_context_tags(source: Source) -> list[str]:
    tags: set[str] = set()
    event_model = _source_event_model(source)
    if event_model:
        tags.add(event_model)

    content_type = source.content_type.lower()
    if content_type == "review":
        tags.add("book_review_source")
    elif content_type == "news":
        tags.add("publishing_news_source")
    elif content_type == "community":
        tags.add("community_book_signal")

    if source.trust_tier.startswith("T1"):
        tags.add("official_book_source")
    return sorted(tags)


def _source_event_model(source: Source) -> str:
    raw = source.config.get("event_model")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()

    content_type = source.content_type.lower()
    if content_type in {"bestseller", "sales_ranking"}:
        return "sales_ranking"
    if content_type in {"library_lending", "lending"}:
        return "library_lending"
    if content_type in {"author_event", "event"}:
        return "author_event"
    if content_type in {"award", "award_signal"}:
        return "award_signal"
    return ""


def _has_book_context(source: Source) -> bool:
    return bool(_source_context_tags(source))


def _has_book_entity(article: Article) -> bool:
    for entity_name, values in article.matched_entities.items():
        if entity_name in BOOK_ENTITY_NAMES and isinstance(values, list) and values:
            return True
    return False


def _is_invalid(article: Article) -> bool:
    text = f"{article.title} {article.summary}"
    return INVALID_CONTENT_RE.search(text) is not None
