from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import yaml

from bookradar.models import Article
from bookradar.storage import RadarStorage


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "backfill_data_quality.py"
    spec = importlib.util.spec_from_file_location("bookradar_backfill_data_quality", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_project_config(project_root: Path) -> None:
    (project_root / "config" / "categories").mkdir(parents=True)
    (project_root / "config" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "database_path": "data/radar_data.duckdb",
                "report_dir": "reports",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (project_root / "config" / "categories" / "book.yaml").write_text(
        yaml.safe_dump(
            {
                "category_name": "book",
                "display_name": "Book Radar",
                "sources": [
                    {
                        "name": "알라딘 베스트셀러",
                        "type": "rss",
                        "url": "https://example.com/bestseller.xml",
                        "content_type": "bestseller",
                        "trust_tier": "T1_official",
                        "enabled": True,
                        "config": {"event_model": "sales_ranking"},
                    },
                    {
                        "name": "Publishers Weekly",
                        "type": "rss",
                        "url": "https://example.com/pw.xml",
                        "content_type": "news",
                        "enabled": True,
                        "config": {"event_model": "editorial_coverage"},
                    },
                ],
                "entities": [
                    {
                        "name": "BookType",
                        "display_name": "Book Type",
                        "keywords": ["베스트셀러", "bestseller"],
                    },
                    {
                        "name": "Publisher",
                        "display_name": "Publisher",
                        "keywords": ["알에이치코리아"],
                    },
                ],
                "data_quality": {
                    "quality_outputs": {"tracked_event_models": ["sales_ranking"]}
                },
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_run_backfill_updates_summaries_and_splits_bestseller_rows(tmp_path: Path) -> None:
    _write_project_config(tmp_path)
    db_path = tmp_path / "data" / "radar_data.duckdb"
    published = datetime(2026, 5, 20, 8, 0, tzinfo=UTC)
    with RadarStorage(db_path) as storage:
        storage.upsert_articles(
            [
                Article(
                    title="Publishers Weekly title-only item",
                    link="https://example.com/pw/1",
                    summary="",
                    published=published,
                    source="Publishers Weekly",
                    category="book",
                ),
                Article(
                    title="[알라딘 베스트 RSS] 도서종합 분야 주간 베스트셀러",
                    link="https://example.com/weekly?week=2026053",
                    summary=(
                        "2026년 5월 3주 베스트셀러 순위입니다."
                        "[주간 베스트셀러 1위]"
                        '<table><tr><td><h2><a href="https://example.com/book">프로젝트 헤일메리</a></h2>'
                        '앤디 위어 / 알에이치코리아 / ISBN:<span class="isbn13">9788925588735</span>'
                        "</td></tr></table>"
                        "[주간 베스트셀러 2위]"
                        '<table><tr><td><h2><a href="https://example.com/book-2">포켓몬 생태도감</a></h2>'
                        '주식회사 포켓몬 / 대원씨아이 / ISBN:<span class="isbn13">9791142350283</span>'
                        "</td></tr></table>"
                    ),
                    published=published,
                    source="알라딘 베스트셀러",
                    category="book",
                ),
            ]
        )

    module = _load_script_module()
    result = module.run_backfill(
        tmp_path,
        create_backup=False,
        write_quality=False,
    )

    assert result.blank_summaries_updated == 1
    assert result.aggregate_ranking_rows_found == 1
    assert result.ranking_items_upserted == 2
    assert result.aggregate_ranking_rows_deleted == 1

    with duckdb.connect(str(db_path), read_only=True) as con:
        missing_summary_count = con.execute(
            "SELECT COUNT(*) FROM articles WHERE summary IS NULL OR length(trim(summary)) = 0"
        ).fetchone()[0]
        weekly_count = con.execute(
            "SELECT COUNT(*) FROM articles WHERE link = 'https://example.com/weekly?week=2026053'"
        ).fetchone()[0]
        ranking_rows = con.execute(
            """
            SELECT title, link, summary, entities_json
            FROM articles
            WHERE source = '알라딘 베스트셀러'
            ORDER BY title
            """
        ).fetchall()

    assert missing_summary_count == 0
    assert weekly_count == 0
    assert [row[0] for row in ranking_rows] == [
        "1위 프로젝트 헤일메리",
        "2위 포켓몬 생태도감",
    ]
    assert "radar_ranking_date=20260520080000" in ranking_rows[0][1]
    assert "9788925588735" in ranking_rows[0][2]
    entities = json.loads(ranking_rows[0][3])
    assert entities["SourceSignal"] == ["official_book_source", "sales_ranking"]
