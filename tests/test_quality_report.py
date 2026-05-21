from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from bookradar.models import Article, CategoryConfig, Source
from bookradar.quality_report import build_quality_report, write_quality_report


def test_build_quality_report_tracks_book_event_statuses() -> None:
    now = datetime(2026, 4, 13, tzinfo=UTC)
    category = CategoryConfig(
        category_name="book",
        display_name="Book Radar",
        sources=[
            Source(
                name="Bookstore Ranking",
                type="rss",
                url="https://example.com/ranking",
                content_type="bestseller",
            ),
            Source(
                name="Book Reviews",
                type="rss",
                url="https://example.com/reviews",
                content_type="review",
            ),
        ],
        entities=[],
    )

    report = build_quality_report(
        category=category,
        articles=[
            Article(
                title="Bestseller update",
                link="https://example.com/ranking/1",
                summary="The weekly bestseller list.",
                published=now - timedelta(days=1),
                source="Bookstore Ranking",
                category="book",
                matched_entities={"BookType": ["bestseller"]},
            ),
            Article(
                title="Award winner interview",
                link="https://example.com/reviews/1",
                summary="A prize-winning novelist speaks.",
                published=now - timedelta(days=2),
                source="Book Reviews",
                category="book",
                matched_entities={"Award": ["award"], "Author": ["author"]},
            ),
        ],
        quality_config={
            "data_quality": {
                "quality_outputs": {
                    "tracked_event_models": [
                        "sales_ranking",
                        "library_lending",
                        "author_event",
                        "award_signal",
                    ]
                },
                "freshness_sla": {"sales_ranking_days": 3, "award_signal_days": 30},
            }
        },
        generated_at=now,
    )

    summary = report["summary"]
    assert summary["tracked_sources"] == 1
    assert summary["fresh_sources"] == 1
    assert summary["not_tracked_sources"] == 1
    assert summary["sales_ranking_events"] == 1
    assert summary["award_signal_events"] == 1
    assert summary["book_signal_event_count"] == 2
    assert summary["event_required_field_gap_count"] >= 1
    assert summary["daily_review_item_count"] >= 1
    assert report["events"][0]["canonical_key"]
    assert "required_field_gaps" in report["events"][0]


def test_build_quality_report_counts_only_enabled_tracked_sources() -> None:
    now = datetime(2026, 5, 21, tzinfo=UTC)
    category = CategoryConfig(
        category_name="book",
        display_name="Book Radar",
        sources=[
            Source(
                name="알라딘 베스트셀러",
                type="rss",
                url="https://example.com/aladin",
                content_type="bestseller",
                enabled=True,
            ),
            Source(
                name="YES24 베스트셀러",
                type="rss",
                url="https://example.com/yes24",
                content_type="bestseller",
                enabled=False,
            ),
        ],
        entities=[],
    )

    report = build_quality_report(
        category=category,
        articles=[
            Article(
                title="1위 프로젝트 헤일메리",
                link="https://example.com/aladin/1",
                summary="ISBN:9788925588735",
                published=now,
                source="알라딘 베스트셀러",
                category="book",
            )
        ],
        quality_config={
            "data_quality": {
                "quality_outputs": {"tracked_event_models": ["sales_ranking"]},
            }
        },
        generated_at=now,
    )

    assert report["summary"]["enabled_sources"] == 1
    assert report["summary"]["tracked_sources"] == 1
    assert report["summary"]["skipped_disabled_sources"] == 1


def test_build_quality_report_reads_rank_before_korean_rank_suffix() -> None:
    now = datetime(2026, 5, 20, tzinfo=UTC)
    category = CategoryConfig(
        category_name="book",
        display_name="Book Radar",
        sources=[
            Source(
                name="알라딘 베스트셀러",
                type="rss",
                url="https://example.com/bestseller",
                content_type="bestseller",
                config={"event_model": "sales_ranking"},
            )
        ],
        entities=[],
    )

    report = build_quality_report(
        category=category,
        articles=[
            Article(
                title="1위 프로젝트 헤일메리",
                link="https://example.com/book-1",
                summary="ISBN:9788925588735",
                published=now,
                source="알라딘 베스트셀러",
                category="book",
                matched_entities={"BookType": ["bestseller"]},
            )
        ],
        quality_config={
            "data_quality": {
                "quality_outputs": {"tracked_event_models": ["sales_ranking"]},
            }
        },
        generated_at=now,
    )

    event = report["events"][0]
    assert report["summary"]["sales_ranking_events"] == 1
    assert event["rank"] == 1
    assert event["isbn"] == "9788925588735"
    assert event["required_field_gaps"] == []


def test_write_quality_report_writes_latest_and_dated_files(tmp_path: Path) -> None:
    report = {
        "category": "book",
        "generated_at": "2026-04-13T00:00:00+00:00",
        "summary": {},
    }

    paths = write_quality_report(report, output_dir=tmp_path, category_name="book")

    assert paths["latest"] == tmp_path / "book_quality.json"
    assert paths["dated"] == tmp_path / "book_20260413_quality.json"
    assert paths["latest"].exists()
    assert paths["dated"].exists()
