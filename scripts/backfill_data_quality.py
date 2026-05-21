#!/usr/bin/env python3
"""Backfill existing BookRadar rows after collector and quality-rule changes."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT.parent / "radar-core"))

from radar_core.ontology import annotate_articles_with_ontology  # noqa: E402
from radar_core.url_utils import canonical_url  # noqa: E402

from bookradar.analyzer import apply_entity_rules  # noqa: E402
from bookradar.collector import _expand_bestseller_entry  # noqa: E402
from bookradar.config_loader import load_category_config  # noqa: E402
from bookradar.models import Article, CategoryConfig, Source  # noqa: E402
from bookradar.relevance import apply_source_context_entities  # noqa: E402
from bookradar.storage import _article_from_row  # noqa: E402
from scripts.check_quality import generate_quality_artifacts  # noqa: E402


@dataclass
class BackfillResult:
    database_path: str
    backup_path: str
    blank_summaries_updated: int
    aggregate_ranking_rows_found: int
    ranking_items_upserted: int
    aggregate_ranking_rows_deleted: int
    metadata_rows_reclassified: int
    quality_report_path: str


def _project_path(project_root: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _load_runtime_config(project_root: Path) -> dict[str, Any]:
    config_path = project_root / "config" / "config.yaml"
    if not config_path.exists():
        return {}
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return raw if isinstance(raw, dict) else {}


def _utc_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def _backup_database(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"{db_path.stem}_{stamp}{db_path.suffix}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _fetch_articles(
    con: duckdb.DuckDBPyConnection,
    *,
    category_name: str,
    source_names: set[str] | None = None,
) -> list[Article]:
    rows = con.execute(
        """
        SELECT
            category,
            source,
            title,
            link,
            summary,
            published,
            collected_at,
            entities_json,
            ontology_json
        FROM articles
        WHERE category = ?
        """,
        [category_name],
    ).fetchall()
    articles = [_article_from_row(row) for row in rows]
    if source_names is None:
        return articles
    return [article for article in articles if article.source in source_names]


def _prepare_articles(
    articles: list[Article],
    *,
    category_cfg: CategoryConfig,
    sources: list[Source],
    project_root: Path,
) -> list[Article]:
    if not articles:
        return []
    annotated = annotate_articles_with_ontology(
        articles,
        repo_name="BookRadar",
        sources_by_name={source.name: source for source in sources},
        category_name=category_cfg.category_name,
        search_from=project_root / "main.py",
        attach_event_model_payload=True,
    )
    analyzed = apply_entity_rules(annotated, category_cfg.entities)
    return apply_source_context_entities(analyzed, sources)


def _update_article_payloads(
    con: duckdb.DuckDBPyConnection,
    articles: list[Article],
) -> int:
    rows = [
        (
            article.summary,
            json.dumps(article.matched_entities, ensure_ascii=False),
            json.dumps(article.ontology, ensure_ascii=False),
            canonical_url(article.link) or article.link,
        )
        for article in articles
    ]
    if not rows:
        return 0
    con.executemany(
        """
        UPDATE articles
        SET summary = ?, entities_json = ?, ontology_json = ?
        WHERE link = ?
        """,
        rows,
    )
    return len(rows)


def _ranking_aggregate_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    category_name: str,
    source_name: str,
) -> list[Article]:
    rows = con.execute(
        """
        SELECT
            category,
            source,
            title,
            link,
            summary,
            published,
            collected_at,
            entities_json,
            ontology_json
        FROM articles
        WHERE category = ?
          AND source = ?
          AND title LIKE '[알라딘 베스트 RSS]%'
          AND summary LIKE '%[주간 베스트셀러%'
        ORDER BY COALESCE(published, collected_at)
        """,
        [category_name, source_name],
    ).fetchall()
    return [_article_from_row(row) for row in rows]


def _expand_ranking_rows(
    aggregates: list[Article],
    *,
    source: Source,
) -> tuple[list[Article], list[str]]:
    expanded: list[Article] = []
    delete_links: list[str] = []
    for aggregate in aggregates:
        ranking_items = _expand_bestseller_entry(
            source=source,
            title=aggregate.title,
            link=aggregate.link,
            summary=aggregate.summary,
            published=aggregate.published,
            category=aggregate.category,
        )
        if not ranking_items:
            continue
        for item in ranking_items:
            item.collected_at = aggregate.collected_at
            expanded.append(item)
        delete_links.append(aggregate.link)
    return expanded, delete_links


def _upsert_backfilled_articles(
    con: duckdb.DuckDBPyConnection,
    articles: list[Article],
    *,
    run_id: str,
) -> int:
    if not articles:
        return 0
    now = _utc_naive(datetime.now(UTC))
    rows = [
        (
            article.category,
            article.source,
            article.title,
            canonical_url(article.link) or article.link,
            article.summary,
            _utc_naive(article.published),
            _utc_naive(article.collected_at) or now,
            json.dumps(article.matched_entities, ensure_ascii=False),
            json.dumps(article.ontology, ensure_ascii=False),
            run_id,
            "bookradar-backfill",
            "backfilled",
            now,
        )
        for article in articles
    ]
    con.executemany(
        """
        INSERT INTO articles (
            category,
            source,
            title,
            link,
            summary,
            published,
            collected_at,
            entities_json,
            ontology_json,
            run_id,
            collector_version,
            fetch_status,
            fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(link) DO UPDATE SET
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            published = EXCLUDED.published,
            entities_json = EXCLUDED.entities_json,
            ontology_json = EXCLUDED.ontology_json,
            run_id = EXCLUDED.run_id,
            collector_version = EXCLUDED.collector_version,
            fetch_status = EXCLUDED.fetch_status,
            fetched_at = EXCLUDED.fetched_at
        """,
        rows,
    )
    return len(rows)


def _delete_links(con: duckdb.DuckDBPyConnection, links: list[str]) -> int:
    if not links:
        return 0
    con.executemany("DELETE FROM articles WHERE link = ?", [(link,) for link in links])
    return len(links)


def run_backfill(
    project_root: Path = PROJECT_ROOT,
    *,
    category_name: str = "book",
    create_backup: bool = True,
    write_quality: bool = True,
) -> BackfillResult:
    runtime_config = _load_runtime_config(project_root)
    db_path = _project_path(
        project_root,
        str(runtime_config.get("database_path", "data/radar_data.duckdb")),
    )
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    backup_path: Path | None = None
    if create_backup:
        backup_path = _backup_database(db_path, project_root / "data" / "backups")

    category_cfg = load_category_config(
        category_name,
        categories_dir=project_root / "config" / "categories",
    )
    enabled_sources = [source for source in category_cfg.sources if source.enabled]
    enabled_source_names = {source.name for source in enabled_sources}
    source_by_name = {source.name: source for source in category_cfg.sources}
    aladin_bestseller = source_by_name.get("알라딘 베스트셀러")
    run_id = f"backfill-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    with duckdb.connect(str(db_path)) as con:
        con.begin()
        try:
            blank_summaries_updated = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM articles
                    WHERE category = ?
                      AND (summary IS NULL OR length(trim(summary)) = 0)
                      AND title IS NOT NULL
                      AND length(trim(title)) > 0
                    """,
                    [category_cfg.category_name],
                ).fetchone()[0]
            )
            _ = con.execute(
                """
                UPDATE articles
                SET summary = title
                WHERE category = ?
                  AND (summary IS NULL OR length(trim(summary)) = 0)
                  AND title IS NOT NULL
                  AND length(trim(title)) > 0
                """,
                [category_cfg.category_name],
            )

            aggregates = []
            prepared_ranking_items = []
            aggregate_links_to_delete = []
            if aladin_bestseller is not None:
                aggregates = _ranking_aggregate_rows(
                    con,
                    category_name=category_cfg.category_name,
                    source_name=aladin_bestseller.name,
                )
                ranking_items, aggregate_links_to_delete = _expand_ranking_rows(
                    aggregates,
                    source=aladin_bestseller,
                )
                prepared_ranking_items = _prepare_articles(
                    ranking_items,
                    category_cfg=category_cfg,
                    sources=enabled_sources,
                    project_root=project_root,
                )
                _ = _upsert_backfilled_articles(
                    con,
                    prepared_ranking_items,
                    run_id=run_id,
                )
                deleted_aggregate_rows = _delete_links(con, aggregate_links_to_delete)
            else:
                deleted_aggregate_rows = 0

            enabled_articles = _fetch_articles(
                con,
                category_name=category_cfg.category_name,
                source_names=enabled_source_names,
            )
            prepared_existing = _prepare_articles(
                enabled_articles,
                category_cfg=category_cfg,
                sources=enabled_sources,
                project_root=project_root,
            )
            metadata_rows_reclassified = _update_article_payloads(con, prepared_existing)
            con.commit()
        except Exception:
            con.rollback()
            raise

    quality_path = ""
    if write_quality:
        paths, _report = generate_quality_artifacts(
            project_root,
            category_name=category_cfg.category_name,
        )
        quality_path = str(paths["latest"])

    return BackfillResult(
        database_path=str(db_path),
        backup_path=str(backup_path or ""),
        blank_summaries_updated=blank_summaries_updated,
        aggregate_ranking_rows_found=len(aggregates),
        ranking_items_upserted=len(prepared_ranking_items),
        aggregate_ranking_rows_deleted=deleted_aggregate_rows,
        metadata_rows_reclassified=metadata_rows_reclassified,
        quality_report_path=quality_path,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill BookRadar data quality fixes")
    _ = parser.add_argument("--category", default="book")
    _ = parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    _ = parser.add_argument("--no-backup", action="store_true")
    _ = parser.add_argument("--no-quality", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_backfill(
        args.project_root,
        category_name=args.category,
        create_backup=not args.no_backup,
        write_quality=not args.no_quality,
    )
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
