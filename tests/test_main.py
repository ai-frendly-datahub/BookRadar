from __future__ import annotations

from pathlib import Path

import main as radar_main
from bookradar.models import Article, CategoryConfig, RadarSettings, Source


def test_run_orchestrates_pipeline_with_storage_and_reports(tmp_path, monkeypatch, capsys) -> None:
    source = Source(name="BookSource", type="rss", url="https://example.com/feed")
    category = CategoryConfig(
        category_name="book",
        display_name="Book Radar",
        sources=[source],
        entities=[],
    )
    collected = Article(
        title="Collected",
        link="https://example.com/collected",
        summary="book",
        published=None,
        source="BookSource",
        category="book",
        matched_entities={"BookType": ["book"]},
    )
    recent = Article(
        title="Recent",
        link="https://example.com/recent",
        summary="book",
        published=None,
        source="BookSource",
        category="book",
        matched_entities={"BookType": ["book"]},
    )
    settings = RadarSettings(
        database_path=tmp_path / "radar.duckdb",
        report_dir=tmp_path / "reports",
        raw_data_dir=tmp_path / "raw",
        search_db_path=tmp_path / "search.db",
    )
    calls: dict[str, object] = {}

    class FakeRawLogger:
        def __init__(self, raw_data_dir: Path) -> None:
            calls["raw_data_dir"] = raw_data_dir

        def log(self, articles, *, source_name: str):
            calls["raw_logged"] = (source_name, list(articles))
            return Path("raw.jsonl")

    class FakeStorage:
        def __init__(self, db_path: Path) -> None:
            calls["db_path"] = db_path

        def upsert_articles(self, articles) -> None:
            calls["upserted"] = list(articles)

        def delete_older_than(self, days: int) -> int:
            calls["keep_days"] = days
            return 0

        def recent_articles(self, category_name: str, *, days: int, limit: int):
            calls.setdefault("recent_calls", []).append((category_name, days, limit))
            return [recent]

        def recent_articles_by_collected_at(self, category_name: str, *, days: int, limit: int):
            calls.setdefault("collected_calls", []).append((category_name, days, limit))
            return []

        def close(self) -> None:
            calls["closed"] = True

    def fake_collect(sources, **kwargs):
        calls["collect"] = (list(sources), kwargs)
        return [collected], ["BookSource: transient"]

    def fake_generate_report(**kwargs):
        calls["report_stats"] = kwargs["stats"]
        kwargs["output_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["output_path"].write_text("<html></html>", encoding="utf-8")
        return kwargs["output_path"]

    monkeypatch.setattr(radar_main, "configure_logging", lambda: None)
    monkeypatch.setattr(radar_main, "load_settings", lambda config_path=None: settings)
    monkeypatch.setattr(radar_main, "load_category_config", lambda *args, **kwargs: category)
    monkeypatch.setattr(radar_main, "load_category_quality_config", lambda *args, **kwargs: {})
    monkeypatch.setattr(radar_main, "collect_sources", fake_collect)
    monkeypatch.setattr(radar_main, "annotate_articles_with_ontology", lambda articles, **kwargs: articles)
    monkeypatch.setattr(radar_main, "RawLogger", FakeRawLogger)
    monkeypatch.setattr(radar_main, "apply_entity_rules", lambda articles, entities: list(articles))
    monkeypatch.setattr(
        radar_main,
        "apply_source_context_entities",
        lambda articles, sources: list(articles),
    )
    monkeypatch.setattr(radar_main, "filter_relevant_articles", lambda articles, sources: list(articles))
    monkeypatch.setattr(radar_main, "RadarStorage", FakeStorage)
    monkeypatch.setattr(radar_main, "build_quality_report", lambda **kwargs: {"summary": {}})
    monkeypatch.setattr(radar_main, "generate_report", fake_generate_report)
    monkeypatch.setattr(
        radar_main,
        "write_quality_report",
        lambda *args, **kwargs: {"latest": tmp_path / "reports" / "book_quality.json"},
    )
    monkeypatch.setattr(radar_main, "generate_index_html", lambda report_dir: report_dir / "index.html")
    monkeypatch.setattr(
        radar_main,
        "apply_date_storage_policy",
        lambda **kwargs: {"snapshot_path": str(tmp_path / "snap.duckdb")},
    )

    output = radar_main.run(
        category="book",
        per_source_limit=3,
        recent_days=5,
        timeout=9,
        keep_days=30,
        snapshot_db=True,
        exclude_sources=[],
    )

    assert output == tmp_path / "reports" / "book_report.html"
    assert calls["collect"][1]["health_db_path"] == str(settings.database_path)
    assert calls["collect"][1]["limit_per_source"] == 3
    assert calls["collect"][1]["timeout"] == 9
    assert calls["raw_logged"][0] == "BookSource"
    assert calls["upserted"] == [collected]
    assert calls["keep_days"] == 30
    assert calls["closed"] is True
    assert calls["report_stats"]["article_count"] == 1
    assert "[Radar] Snapshot saved at" in capsys.readouterr().out


def test_main_conversion_helpers() -> None:
    path = Path("config.yaml")

    assert radar_main._to_path(path) == path
    assert radar_main._to_path("config.yaml") is None
    assert radar_main._to_int(True, 7) == 7
    assert radar_main._to_int("12", 7) == 12
    assert radar_main._to_int("bad", 7) == 7
    assert radar_main._to_optional_int(None) is None
    assert radar_main._to_optional_int(False) is None
    assert radar_main._to_optional_int("13") == 13
    assert radar_main._to_optional_int("bad") is None
    assert radar_main._to_str_list(["a", 3, "b"]) == ["a", "b"]
    assert radar_main._to_str_list("a") == []
