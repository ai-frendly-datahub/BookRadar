from __future__ import annotations

from datetime import UTC, datetime

import pytest

from bookradar.models import Article, CategoryConfig
from bookradar.reporter import generate_index_html, generate_report


@pytest.fixture()
def fixed_now():
    return datetime(2024, 3, 15, 9, 30, tzinfo=UTC)


@pytest.fixture()
def patch_datetime(monkeypatch, fixed_now):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("radar_core.report_utils.datetime", FixedDateTime)


@pytest.fixture()
def report_articles(fixed_now):
    return [
        Article(
            title="Book Launch",
            link="https://example.com/book1",
            summary="New bestseller released.",
            published=fixed_now,
            source="BookNews",
            category="book",
            matched_entities={"Author": ["author"]},
            collected_at=fixed_now,
        ),
    ]


@pytest.fixture()
def report_category():
    return CategoryConfig(
        category_name="book",
        display_name="Book Radar",
        sources=[],
        entities=[],
    )


@pytest.fixture()
def report_stats():
    return {"sources": 1, "collected": 1, "matched": 1, "window_days": 7}


class TestGenerateReport:
    """Unit tests for generate_report."""

    def test_generate_report_creates_file(
        self, tmp_path, report_category, report_articles, report_stats, patch_datetime
    ):
        """Report file is created at the specified path."""
        output = tmp_path / "reports" / "book_report.html"
        result = generate_report(
            category=report_category,
            articles=report_articles,
            output_path=output,
            stats=report_stats,
        )
        assert result == output
        assert output.exists()

    def test_generate_report_html_content(
        self, tmp_path, report_category, report_articles, report_stats, patch_datetime
    ):
        """Generated HTML contains expected content."""
        output = tmp_path / "reports" / "book_report.html"
        generate_report(
            category=report_category,
            articles=report_articles,
            output_path=output,
            stats=report_stats,
        )
        html = output.read_text(encoding="utf-8")
        assert "Book Radar" in html
        assert "Book Launch" in html

    def test_generate_report_with_errors(
        self, tmp_path, report_category, report_articles, report_stats, patch_datetime
    ):
        """Error messages appear in the report HTML."""
        output = tmp_path / "reports" / "book_report.html"
        generate_report(
            category=report_category,
            articles=report_articles,
            output_path=output,
            stats=report_stats,
            errors=["source timeout"],
        )
        html = output.read_text(encoding="utf-8")
        assert "source timeout" in html

    def test_generate_report_injects_book_quality_panel(
        self, tmp_path, report_category, report_articles, report_stats, patch_datetime
    ):
        """Book quality telemetry appears when provided."""
        output = tmp_path / "reports" / "book_report.html"
        generate_report(
            category=report_category,
            articles=report_articles,
            output_path=output,
            stats=report_stats,
            quality_report={
                "summary": {
                    "book_signal_event_count": 1,
                    "sales_ranking_events": 1,
                    "event_required_field_gap_count": 2,
                },
                "events": [
                    {
                        "event_model": "sales_ranking",
                        "source": "Bookstore Ranking",
                        "canonical_key": "book_edition:9781234567890",
                        "canonical_key_status": "complete",
                        "required_field_gaps": [],
                    }
                ],
                "daily_review_items": [],
            },
        )
        html = output.read_text(encoding="utf-8")
        assert 'id="book-quality"' in html
        assert "Book Quality" in html
        assert "book_edition:9781234567890" in html
        summaries = sorted(
            (tmp_path / "reports").glob(
                "book_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_summary.json"
            )
        )
        assert len(summaries) == 1
        summary = summaries[0].read_text(encoding="utf-8")
        assert '"repo": "BookRadar"' in summary
        assert '"ontology_version": "0.1.0"' in summary
        assert '"book.sales_ranking"' in summary


class TestGenerateIndexHtml:
    """Unit tests for generate_index_html."""

    def test_generate_index_html(self, tmp_path):
        """Index HTML is generated listing report files."""
        report_dir = tmp_path / "reports"
        report_dir.mkdir(parents=True)
        (report_dir / "book_20240315.html").write_text("<html>book</html>", encoding="utf-8")

        index_path = generate_index_html(report_dir)

        assert index_path == report_dir / "index.html"
        assert index_path.exists()
        rendered = index_path.read_text(encoding="utf-8")
        assert "Book Radar" in rendered
        assert "book_20240315.html" in rendered

    def test_generate_index_html_empty_dir(self, tmp_path):
        """Index is generated even with no reports."""
        report_dir = tmp_path / "empty_reports"
        index_path = generate_index_html(report_dir)

        assert index_path.exists()
        rendered = index_path.read_text(encoding="utf-8")
        assert "Book Radar" in rendered
