from __future__ import annotations

from collections.abc import Iterable, Mapping
from html import escape
from pathlib import Path
from typing import Any

from radar_core.ontology import build_summary_ontology_metadata
from radar_core.report_utils import (
    generate_index_html as _core_generate_index_html,
)
from radar_core.report_utils import (
    generate_report as _core_generate_report,
)

from .models import Article, CategoryConfig


def generate_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    output_path: Path,
    stats: dict[str, int],
    errors: list[str] | None = None,
    store=None,
    quality_report: Mapping[str, Any] | None = None,
) -> Path:
    """Generate HTML report (delegates to radar-core)."""
    articles_list = list(articles)
    plugin_charts = []

    # --- Universal plugins (entity heatmap + source reliability) ---
    try:
        from radar_core.plugins.entity_heatmap import get_chart_config as _heatmap_config

        _heatmap = _heatmap_config(articles=articles_list)
        if _heatmap is not None:
            plugin_charts.append(_heatmap)
    except Exception:
        pass
    try:
        from radar_core.plugins.source_reliability import get_chart_config as _reliability_config

        _reliability = _reliability_config(store=store)
        if _reliability is not None:
            plugin_charts.append(_reliability)
    except Exception:
        pass

    report_path = _core_generate_report(
        category=category,
        articles=articles_list,
        output_path=output_path,
        stats=stats,
        errors=errors,
        plugin_charts=plugin_charts if plugin_charts else None,
        ontology_metadata=build_summary_ontology_metadata(
            "BookRadar",
            category_name=category.category_name,
            search_from=Path(__file__).resolve(),
        ),
    )
    if quality_report:
        _inject_book_quality_panel(report_path, quality_report)
    return report_path


def generate_index_html(
    report_dir: Path,
    summaries_dir: Path | None = None,
) -> Path:
    """Generate index.html (delegates to radar-core)."""
    radar_name = "Book Radar"
    return _core_generate_index_html(report_dir, radar_name)


def _inject_book_quality_panel(report_path: Path, quality_report: Mapping[str, Any]) -> None:
    if not report_path.exists():
        return
    html = report_path.read_text(encoding="utf-8")
    panel = _render_book_quality_panel(quality_report)
    marker = "</body>"
    if marker in html:
        html = html.replace(marker, panel + "\n" + marker, 1)
    else:
        html += "\n" + panel
    report_path.write_text(html, encoding="utf-8")


def _render_book_quality_panel(quality_report: Mapping[str, Any]) -> str:
    summary = _mapping(quality_report.get("summary"))
    events = _list_of_mappings(quality_report.get("events"))
    review_items = _list_of_mappings(quality_report.get("daily_review_items"))
    cards = [
        ("Book signals", summary.get("book_signal_event_count", 0)),
        ("Sales ranking", summary.get("sales_ranking_events", 0)),
        ("Library lending", summary.get("library_lending_events", 0)),
        ("Author events", summary.get("author_event_events", 0)),
        ("Award signals", summary.get("award_signal_events", 0)),
        ("ISBN present", summary.get("isbn_present_count", 0)),
        ("Required gaps", summary.get("event_required_field_gap_count", 0)),
        ("Review items", summary.get("daily_review_item_count", 0)),
    ]
    cards_html = "\n".join(
        "<div class=\"book-quality-card\">"
        f"<span>{escape(label)}</span><strong>{escape(str(value))}</strong>"
        "</div>"
        for label, value in cards
    )
    return f"""
<section id="book-quality" class="book-quality-panel">
  <style>
    .book-quality-panel {{ margin: 32px auto; max-width: 1180px; padding: 24px; border: 1px solid #d8dee4; border-radius: 8px; background: #fff; color: #24292f; }}
    .book-quality-panel h2 {{ margin: 0 0 8px; font-size: 1.35rem; }}
    .book-quality-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin: 16px 0 22px; }}
    .book-quality-card {{ border: 1px solid #d8dee4; border-radius: 8px; padding: 10px 12px; background: #f6f8fa; }}
    .book-quality-card span {{ display: block; font-size: .82rem; color: #57606a; }}
    .book-quality-card strong {{ display: block; margin-top: 4px; font-size: 1.2rem; }}
    .book-quality-table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: .9rem; }}
    .book-quality-table th, .book-quality-table td {{ border-top: 1px solid #d8dee4; padding: 8px; text-align: left; vertical-align: top; }}
    .book-quality-review {{ margin: 12px 0 0; padding-left: 18px; }}
  </style>
  <h2>Book Quality</h2>
  <p>Edition, ranking, lending, author-event, and award evidence is tracked separately from review context.</p>
  <div class="book-quality-grid">
    {cards_html}
  </div>
  {_render_quality_events(events)}
  {_render_quality_review(review_items)}
</section>
""".strip()


def _render_quality_events(events: list[Mapping[str, Any]]) -> str:
    if not events:
        return "<p>No book quality events were observed in this report window.</p>"
    rows = []
    for event in events[:10]:
        gaps = ", ".join(str(value) for value in event.get("required_field_gaps") or [])
        rows.append(
            "<tr>"
            f"<td>{escape(str(event.get('event_model') or ''))}</td>"
            f"<td>{escape(str(event.get('source') or ''))}</td>"
            f"<td>{escape(str(event.get('canonical_key') or ''))}</td>"
            f"<td>{escape(str(event.get('canonical_key_status') or ''))}</td>"
            f"<td>{escape(gaps)}</td>"
            "</tr>"
        )
    return (
        "<h3>Observed Events</h3>"
        "<table class=\"book-quality-table\"><thead><tr>"
        "<th>Model</th><th>Source</th><th>Canonical key</th><th>Status</th><th>Gaps</th>"
        "</tr></thead><tbody>"
        + "\n".join(rows)
        + "</tbody></table>"
    )


def _render_quality_review(review_items: list[Mapping[str, Any]]) -> str:
    if not review_items:
        return "<p>No daily review items.</p>"
    items = []
    for item in review_items[:10]:
        label = item.get("source") or item.get("event_model") or item.get("signal_type") or ""
        items.append(
            "<li>"
            f"{escape(str(item.get('reason') or 'review'))}: {escape(str(label))}"
            "</li>"
        )
    return "<h3>Daily Review</h3><ul class=\"book-quality-review\">" + "\n".join(items) + "</ul>"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list_of_mappings(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]
