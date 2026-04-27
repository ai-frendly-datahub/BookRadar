from __future__ import annotations

import json
import hashlib
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import Article, CategoryConfig, Source


TRACKED_EVENT_MODEL_ORDER = [
    "sales_ranking",
    "library_lending",
    "author_event",
    "award_signal",
]
TRACKED_EVENT_MODELS = set(TRACKED_EVENT_MODEL_ORDER)


def build_quality_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    errors: Iterable[str] | None = None,
    quality_config: Mapping[str, object] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _as_utc(generated_at or datetime.now(UTC))
    articles_list = list(articles)
    errors_list = [str(error) for error in (errors or [])]
    quality = _dict(quality_config or {}, "data_quality")
    freshness_sla = _dict(quality, "freshness_sla")
    event_model_config = _dict(quality, "event_models")
    tracked_event_models = _tracked_event_models(quality)

    event_rows = _build_event_rows(
        articles=articles_list,
        sources=category.sources,
        tracked_event_models=tracked_event_models,
        event_model_config=event_model_config,
    )
    source_rows = [
        _build_source_row(
            source=source,
            articles=articles_list,
            event_rows=event_rows,
            errors=errors_list,
            freshness_sla=freshness_sla,
            tracked_event_models=tracked_event_models,
            generated_at=generated,
        )
        for source in category.sources
    ]

    status_counts = Counter(str(row["status"]) for row in source_rows)
    event_counts = Counter(str(row["event_model"]) for row in event_rows)
    summary = {
        "total_sources": len(source_rows),
        "enabled_sources": sum(1 for row in source_rows if row["enabled"]),
        "tracked_sources": sum(1 for row in source_rows if row["tracked"]),
        "fresh_sources": status_counts.get("fresh", 0),
        "stale_sources": status_counts.get("stale", 0),
        "missing_sources": status_counts.get("missing", 0),
        "missing_event_sources": status_counts.get("missing_event", 0),
        "unknown_event_date_sources": status_counts.get("unknown_event_date", 0),
        "not_tracked_sources": status_counts.get("not_tracked", 0),
        "skipped_disabled_sources": status_counts.get("skipped_disabled", 0),
        "collection_error_count": len(errors_list),
    }
    for event_model in TRACKED_EVENT_MODEL_ORDER:
        summary[f"{event_model}_events"] = event_counts.get(event_model, 0)
    summary.update(
        _event_quality_summary(
            events=event_rows,
            source_rows=source_rows,
            quality_config=quality_config or {},
            tracked_event_models=tracked_event_models,
        )
    )
    daily_review_items = _daily_review_items(
        events=event_rows,
        source_rows=source_rows,
        quality_config=quality_config or {},
        tracked_event_models=tracked_event_models,
    )
    summary["daily_review_item_count"] = len(daily_review_items)

    return {
        "category": category.category_name,
        "generated_at": generated.isoformat(),
        "scope_note": (
            "BookRadar separates retailer ranking, library lending, author event, "
            "and award evidence from broad publishing news, review, and community feeds. "
            "Community discussion remains contextual until ISBN or event identifiers exist."
        ),
        "summary": summary,
        "sources": source_rows,
        "events": event_rows,
        "daily_review_items": daily_review_items,
        "source_backlog": (quality_config or {}).get("source_backlog", {}),
        "errors": errors_list,
    }


def write_quality_report(
    report: Mapping[str, object],
    *,
    output_dir: Path,
    category_name: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _parse_datetime(str(report.get("generated_at") or "")) or datetime.now(
        UTC
    )
    date_stamp = _as_utc(generated_at).strftime("%Y%m%d")
    latest_path = output_dir / f"{category_name}_quality.json"
    dated_path = output_dir / f"{category_name}_{date_stamp}_quality.json"
    encoded = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    latest_path.write_text(encoded + "\n", encoding="utf-8")
    dated_path.write_text(encoded + "\n", encoding="utf-8")
    return {"latest": latest_path, "dated": dated_path}


def _build_event_rows(
    *,
    articles: list[Article],
    sources: list[Source],
    tracked_event_models: set[str],
    event_model_config: Mapping[str, object],
) -> list[dict[str, Any]]:
    source_map = {source.name: source for source in sources}
    rows: list[dict[str, Any]] = []
    for article in articles:
        source = source_map.get(article.source)
        if source is None:
            continue
        for event_model in _article_event_models(article, source, tracked_event_models):
            event_at = _event_datetime(article)
            row = {
                "source": source.name,
                "source_type": source.type,
                "trust_tier": source.trust_tier,
                "content_type": source.content_type,
                "collection_tier": source.collection_tier,
                "producer_role": source.producer_role,
                "info_purpose": source.info_purpose,
                "event_model": event_model,
                "title": article.title,
                "url": article.link,
                "source_url": article.link or source.url,
                "event_at": event_at.isoformat() if event_at else None,
                "isbn": _isbn(article),
                "rank": _rank(article),
                "retailer": _retailer(article, source),
                "library_network": _library_network(article, source),
                "lending_count": _lending_count(article),
                "venue": _venue(article),
                "award_name": _award_name(article),
                "genre": _matches(article, "Genre"),
                "publisher": _matches(article, "Publisher"),
                "book_type": _matches(article, "BookType"),
                "author": _matches(article, "Author"),
                "award": _matches(article, "Award"),
                "book_event": _matches(article, "BookEvent"),
                "source_signal": _matches(article, "SourceSignal"),
            }
            canonical_key, canonical_key_status = _canonical_key(row)
            row["canonical_key"] = canonical_key
            row["canonical_key_status"] = canonical_key_status
            row["event_key"] = _event_key(row, event_at)
            row["required_field_proxy"] = _required_field_proxy(
                row, event_model, event_model_config
            )
            row["required_field_gaps"] = _required_field_gaps(
                row, event_model, event_model_config
            )
            rows.append(row)
    return rows


def _build_source_row(
    *,
    source: Source,
    articles: list[Article],
    event_rows: list[dict[str, Any]],
    errors: list[str],
    freshness_sla: Mapping[str, object],
    tracked_event_models: set[str],
    generated_at: datetime,
) -> dict[str, Any]:
    source_articles = [article for article in articles if article.source == source.name]
    source_errors = [error for error in errors if error.startswith(f"{source.name}:")]
    event_model = _source_event_model(source)
    source_event_rows = [
        row
        for row in event_rows
        if row["source"] == source.name and row["event_model"] == event_model
    ]
    latest_event = _latest_event(source_event_rows)
    latest_event_at = (
        _parse_datetime(str(latest_event.get("event_at") or "")) if latest_event else None
    )
    sla_days = _source_sla_days(source, event_model, freshness_sla)
    age_days = _age_days(generated_at, latest_event_at) if latest_event_at else None
    status = _source_status(
        source=source,
        event_model=event_model,
        tracked_event_models=tracked_event_models,
        article_count=len(source_articles),
        event_count=len(source_event_rows),
        latest_event_at=latest_event_at,
        sla_days=sla_days,
        age_days=age_days,
    )

    return {
        "source": source.name,
        "source_type": source.type,
        "enabled": source.enabled,
        "trust_tier": source.trust_tier,
        "content_type": source.content_type,
        "collection_tier": source.collection_tier,
        "producer_role": source.producer_role,
        "info_purpose": source.info_purpose,
        "tracked": event_model in tracked_event_models,
        "event_model": event_model,
        "freshness_sla_days": sla_days,
        "status": status,
        "article_count": len(source_articles),
        "event_count": len(source_event_rows),
        "latest_event_at": latest_event_at.isoformat() if latest_event_at else None,
        "age_days": round(age_days, 2) if age_days is not None else None,
        "latest_title": str(latest_event.get("title", "")) if latest_event else "",
        "latest_url": str(latest_event.get("url", "")) if latest_event else "",
        "latest_source_signal": latest_event.get("source_signal", []) if latest_event else [],
        "latest_required_field_proxy": (
            latest_event.get("required_field_proxy", {}) if latest_event else {}
        ),
        "errors": source_errors,
    }


def _article_event_models(
    article: Article,
    source: Source,
    tracked_event_models: set[str],
) -> list[str]:
    values: set[str] = set()
    source_event_model = _source_event_model(source)
    if source_event_model in tracked_event_models:
        values.add(source_event_model)
    if _matches(article, "Award"):
        values.add("award_signal")
    if _matches(article, "BookEvent"):
        values.add("author_event")
    return [event_model for event_model in TRACKED_EVENT_MODEL_ORDER if event_model in values]


def _source_status(
    *,
    source: Source,
    event_model: str,
    tracked_event_models: set[str],
    article_count: int,
    event_count: int,
    latest_event_at: datetime | None,
    sla_days: float | None,
    age_days: float | None,
) -> str:
    if not source.enabled:
        return "skipped_disabled"
    if event_model not in tracked_event_models:
        return "not_tracked"
    if article_count == 0:
        return "missing"
    if event_count == 0:
        return "missing_event"
    if latest_event_at is None or age_days is None:
        return "unknown_event_date"
    if sla_days is not None and age_days > sla_days:
        return "stale"
    return "fresh"


def _tracked_event_models(quality: Mapping[str, object]) -> set[str]:
    outputs = _dict(quality, "quality_outputs")
    raw = outputs.get("tracked_event_models")
    if isinstance(raw, list):
        values = {str(item).strip() for item in raw if str(item).strip()}
        return values & TRACKED_EVENT_MODELS or set(TRACKED_EVENT_MODELS)
    return set(TRACKED_EVENT_MODELS)


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


def _source_sla_days(
    source: Source,
    event_model: str,
    freshness_sla: Mapping[str, object],
) -> float | None:
    raw_source_sla = source.config.get("freshness_sla_days")
    parsed_source_sla = _as_float(raw_source_sla)
    if parsed_source_sla is not None:
        return parsed_source_sla

    suffixed_days = _as_float(freshness_sla.get(f"{event_model}_days"))
    if suffixed_days is not None:
        return suffixed_days

    suffixed_hours = _as_float(freshness_sla.get(f"{event_model}_hours"))
    if suffixed_hours is not None:
        return suffixed_hours / 24
    return None


def _latest_event(event_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    dated: list[tuple[datetime, dict[str, Any]]] = []
    undated: list[dict[str, Any]] = []
    for row in event_rows:
        event_at = _parse_datetime(str(row.get("event_at") or ""))
        if event_at is not None:
            dated.append((event_at, row))
        else:
            undated.append(row)
    if dated:
        return max(dated, key=lambda item: item[0])[1]
    return undated[0] if undated else None


def _event_datetime(article: Article) -> datetime | None:
    article_time = article.published or article.collected_at
    return _as_utc(article_time) if article_time else None


def _event_quality_summary(
    *,
    events: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    quality_config: Mapping[str, object],
    tracked_event_models: set[str],
) -> dict[str, int]:
    event_counts = Counter(str(row.get("event_model") or "") for row in events)
    return {
        "book_signal_event_count": sum(
            event_counts.get(model, 0) for model in tracked_event_models
        ),
        "official_or_operational_event_count": sum(
            1
            for row in events
            if str(row.get("trust_tier") or "").startswith("T1_")
            or str(row.get("source_type") or "").lower() in {"api", "mcp"}
        ),
        "community_proxy_event_count": sum(
            1 for row in events if str(row.get("content_type") or "").lower() == "community"
        ),
        "complete_canonical_key_count": sum(
            1 for row in events if row.get("canonical_key_status") == "complete"
        ),
        "proxy_canonical_key_count": sum(
            1 for row in events if str(row.get("canonical_key_status") or "").endswith("_proxy")
        ),
        "missing_canonical_key_count": sum(1 for row in events if not row.get("canonical_key")),
        "isbn_present_count": sum(1 for row in events if row.get("isbn")),
        "author_present_count": sum(1 for row in events if row.get("author")),
        "publisher_present_count": sum(1 for row in events if row.get("publisher")),
        "rank_present_count": sum(1 for row in events if row.get("rank") is not None),
        "lending_count_present_count": sum(
            1 for row in events if row.get("lending_count") is not None
        ),
        "event_required_field_gap_count": sum(
            len(row.get("required_field_gaps") or []) for row in events
        ),
        "tracked_source_gap_count": sum(
            1
            for row in source_rows
            if row.get("tracked")
            and row.get("status") in {"missing", "missing_event", "unknown_event_date", "stale"}
        ),
        "missing_event_model_count": sum(
            1 for model in tracked_event_models if event_counts.get(model, 0) == 0
        ),
        "source_backlog_candidate_count": len(_source_backlog_items(quality_config)),
    }


def _daily_review_items(
    *,
    events: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    quality_config: Mapping[str, object],
    tracked_event_models: set[str],
) -> list[dict[str, Any]]:
    review: list[dict[str, Any]] = []
    for row in events:
        gaps = [str(value) for value in row.get("required_field_gaps") or []]
        if gaps:
            review.append(
                {
                    "reason": "missing_required_fields",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "canonical_key": row.get("canonical_key"),
                    "required_field_gaps": gaps,
                }
            )
        if str(row.get("canonical_key_status") or "").endswith("_proxy"):
            review.append(
                {
                    "reason": "proxy_canonical_key",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "canonical_key_status": row.get("canonical_key_status"),
                }
            )
        if str(row.get("content_type") or "").lower() == "community":
            review.append(
                {
                    "reason": "community_proxy_source",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                }
            )

    for source in source_rows:
        if source.get("tracked") and source.get("status") in {
            "missing",
            "missing_event",
            "unknown_event_date",
            "stale",
        }:
            review.append(
                {
                    "reason": f"source_{source.get('status')}",
                    "source": source.get("source"),
                    "event_model": source.get("event_model"),
                    "age_days": source.get("age_days"),
                }
            )

    event_counts = Counter(str(row.get("event_model") or "") for row in events)
    for event_model in TRACKED_EVENT_MODEL_ORDER:
        if event_model in tracked_event_models and event_counts.get(event_model, 0) == 0:
            review.append({"reason": "missing_event_model", "event_model": event_model})
    for item in _source_backlog_items(quality_config):
        review.append(
            {
                "reason": "source_backlog_pending",
                "source": item.get("name") or item.get("id"),
                "signal_type": item.get("signal_type"),
                "activation_gate": item.get("activation_gate"),
            }
        )
    return review[:50]


def _source_backlog_items(quality_config: Mapping[str, object]) -> list[Mapping[str, object]]:
    backlog = _dict(quality_config, "source_backlog")
    candidates = backlog.get("operational_candidates")
    if not isinstance(candidates, list):
        return []
    return [item for item in candidates if isinstance(item, Mapping)]


def _required_field_proxy(
    row: Mapping[str, Any],
    event_model: str,
    event_model_config: Mapping[str, object],
) -> dict[str, bool]:
    event_config = _dict(event_model_config, event_model)
    raw_fields = event_config.get("required_fields")
    if not isinstance(raw_fields, list):
        raw_fields = _default_required_fields(event_model)
    return {str(field): _field_present(row, str(field)) for field in raw_fields if str(field).strip()}


def _required_field_gaps(
    row: Mapping[str, Any],
    event_model: str,
    event_model_config: Mapping[str, object],
) -> list[str]:
    return [
        field
        for field, present in _required_field_proxy(row, event_model, event_model_config).items()
        if not present
    ]


def _default_required_fields(event_model: str) -> list[str]:
    if event_model == "sales_ranking":
        return ["isbn", "rank", "retailer"]
    if event_model == "library_lending":
        return ["isbn", "library_network", "lending_count"]
    if event_model == "author_event":
        return ["author_name", "venue", "source_url"]
    if event_model == "award_signal":
        return ["isbn", "award_name", "source_url"]
    return ["source_url"]


def _field_present(row: Mapping[str, Any], field: str) -> bool:
    aliases = {
        "author_name": ("author",),
        "award_name": ("award_name", "award"),
        "source_url": ("source_url", "url"),
        "isbn": ("isbn",),
        "rank": ("rank",),
        "retailer": ("retailer",),
        "library_network": ("library_network",),
        "lending_count": ("lending_count",),
        "venue": ("venue",),
    }
    for alias in aliases.get(field.lower(), (field.lower(),)):
        value = row.get(alias)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, list) and not value:
            continue
        return True
    return False


def _canonical_key(row: Mapping[str, Any]) -> tuple[str, str]:
    event_model = str(row.get("event_model") or "")
    isbn = _slug(row.get("isbn") or "")
    title = _slug(row.get("title") or "")
    author = _slug(_first(row.get("author") if isinstance(row.get("author"), list) else []))
    publisher = _slug(_first(row.get("publisher") if isinstance(row.get("publisher"), list) else []))
    source = _slug(row.get("source") or "")

    if isbn:
        return f"book_edition:{isbn}", "complete"
    if title and author and publisher:
        return f"book_edition:title:{_digest(title + author + publisher)}", "title_proxy"
    if title and author:
        return f"book_edition:title:{_digest(title + author)}", "title_proxy"
    if event_model == "author_event" and author:
        return f"book_author:{author}", "author_proxy"
    if source and title:
        return f"book_source:{source}:{_digest(title)}", "source_proxy"
    return "", "missing"


def _event_key(row: Mapping[str, Any], event_at: datetime | None) -> str:
    observed = _as_utc(event_at).strftime("%Y%m%d") if event_at else "undated"
    basis = row.get("canonical_key") or row.get("source_url") or row.get("title") or ""
    return f"{row.get('event_model')}:{_digest(basis)}:{observed}"


def _isbn(article: Article) -> str:
    text = f"{article.title} {article.summary} {article.link}"
    match = re.search(r"\b(?:97[89][-\s]?)?\d[-\s]?\d{2,5}[-\s]?\d{2,7}[-\s]?[\dX]\b", text)
    return re.sub(r"[-\s]", "", match.group(0)) if match else ""


def _rank(article: Article) -> int | None:
    match = re.search(r"(?:rank|#|위)\s*(\d{1,5})", f"{article.title} {article.summary}", re.I)
    return int(match.group(1)) if match else None


def _retailer(article: Article, source: Source) -> str:
    return source.name if source.content_type in {"bestseller", "sales_ranking"} else ""


def _library_network(article: Article, source: Source) -> str:
    return source.name if source.content_type in {"library_lending", "lending"} else ""


def _lending_count(article: Article) -> int | None:
    match = re.search(r"(\d[\d,]*)\s*(?:loans|lending|대출)", f"{article.title} {article.summary}", re.I)
    return int(match.group(1).replace(",", "")) if match else None


def _venue(article: Article) -> str:
    matches = _matches(article, "BookEvent")
    return matches[0] if matches else ""


def _award_name(article: Article) -> str:
    matches = _matches(article, "Award")
    return matches[0] if matches else ""


def _matches(article: Article, key: str) -> list[str]:
    values = article.matched_entities.get(key, [])
    if isinstance(values, list):
        return [str(value) for value in values]
    return []


def _first(values: list[str]) -> str:
    return values[0] if values else ""


def _dict(mapping: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = mapping.get(key)
    return value if isinstance(value, Mapping) else {}


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _age_days(generated_at: datetime, event_at: datetime) -> float:
    return max(0.0, (_as_utc(generated_at) - _as_utc(event_at)).total_seconds() / 86400)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_datetime(value: str) -> datetime | None:
    if not value or value == "None":
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _slug(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9가-힣]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:120]


def _digest(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
