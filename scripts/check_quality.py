#!/usr/bin/env python3
"""Run DuckDB data quality checks and refresh BookRadar quality JSON."""

from __future__ import annotations

from datetime import UTC, date, datetime
import sys
from pathlib import Path
from typing import Any

import duckdb
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT.parent / "radar-core"))

from bookradar.config_loader import load_category_config, load_category_quality_config  # noqa: E402
from bookradar.quality_report import build_quality_report, write_quality_report  # noqa: E402
from bookradar.relevance import (  # noqa: E402
    apply_source_context_entities,
    filter_relevant_articles,
)
from bookradar.storage import RadarStorage  # noqa: E402
from radar_core.common.quality_checks import (  # noqa: E402
    check_dates,
    check_duplicate_urls,
    check_missing_fields,
    check_text_lengths,
)


def _project_path(project_root: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _load_runtime_config(project_root: Path) -> dict[str, Any]:
    config_path = project_root / "config" / "config.yaml"
    if not config_path.exists():
        return {}
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return raw if isinstance(raw, dict) else {}


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
    return None


def _latest_article_date(db_path: Path, category_name: str) -> date | None:
    if not db_path.exists():
        return None
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            row = con.execute(
                """
                SELECT MAX(COALESCE(published, collected_at))
                FROM articles
                WHERE category = ?
                """,
                [category_name],
            ).fetchone()
    except duckdb.Error:
        return None
    if not row:
        return None
    return _coerce_date(row[0])


def _lookback_days(target_date: date | None, *, minimum_days: int = 14) -> int:
    if target_date is None:
        return minimum_days
    age_days = (datetime.now(UTC).date() - target_date).days + 1
    return max(minimum_days, age_days)


def _run_storage_checks(con: duckdb.DuckDBPyConnection) -> None:
    total = con.execute("SELECT COUNT(*) FROM articles").fetchone()
    total_records = int(total[0]) if total else 0
    print(f"Total records: {total_records}")

    check_missing_fields(
        con,
        table_name="articles",
        null_conditions={
            "title": "title IS NULL OR title = ''",
            "link": "link IS NULL OR link = ''",
            "summary": "summary IS NULL OR summary = ''",
            "published": "published IS NULL",
        },
    )
    check_duplicate_urls(con, table_name="articles", url_column="link")
    check_text_lengths(con, table_name="articles", text_columns=["title", "summary"])
    check_dates(con, table_name="articles", date_column="published")


def generate_quality_artifacts(
    project_root: Path = PROJECT_ROOT,
    *,
    category_name: str = "book",
) -> tuple[dict[str, Path], dict[str, Any]]:
    runtime_config = _load_runtime_config(project_root)
    db_path = _project_path(
        project_root,
        str(runtime_config.get("database_path", "data/radar_data.duckdb")),
    )
    report_dir = _project_path(
        project_root,
        str(runtime_config.get("report_dir", "reports")),
    )
    categories_dir = project_root / "config" / "categories"
    category_cfg = load_category_config(category_name, categories_dir=categories_dir)
    quality_cfg = load_category_quality_config(category_name, categories_dir=categories_dir)
    lookback_days = _lookback_days(_latest_article_date(db_path, category_cfg.category_name))

    with RadarStorage(db_path) as storage:
        articles_by_link = {
            article.link: article
            for article in [
                *storage.recent_articles(
                    category_cfg.category_name,
                    days=lookback_days,
                    limit=1000,
                ),
                *storage.recent_articles_by_collected_at(
                    category_cfg.category_name,
                    days=lookback_days,
                    limit=1000,
                ),
            ]
        }

    articles = list(articles_by_link.values())
    scoped_articles = filter_relevant_articles(
        apply_source_context_entities(articles, category_cfg.sources),
        category_cfg.sources,
    )
    report = build_quality_report(
        category=category_cfg,
        articles=scoped_articles or articles,
        quality_config=quality_cfg,
    )
    paths = write_quality_report(
        report,
        output_dir=report_dir,
        category_name=category_cfg.category_name,
    )
    return paths, report


def main() -> None:
    runtime_config = _load_runtime_config(PROJECT_ROOT)
    db_path = _project_path(
        PROJECT_ROOT,
        str(runtime_config.get("database_path", "data/radar_data.duckdb")),
    )
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    with duckdb.connect(str(db_path), read_only=True) as con:
        _run_storage_checks(con)

    paths, report = generate_quality_artifacts(PROJECT_ROOT)
    summary = report["summary"]
    print(f"quality_report={paths['latest']}")
    print(f"tracked_sources={summary['tracked_sources']}")
    print(f"fresh_sources={summary['fresh_sources']}")
    print(f"stale_sources={summary['stale_sources']}")
    print(f"missing_sources={summary['missing_sources']}")
    print(f"not_tracked_sources={summary['not_tracked_sources']}")
    print(f"book_signal_event_count={summary['book_signal_event_count']}")


if __name__ == "__main__":
    main()
