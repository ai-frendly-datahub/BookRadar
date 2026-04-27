from __future__ import annotations

from pathlib import Path

from bookradar.config_loader import load_category_config, load_category_quality_config


def test_load_category_config_preserves_source_metadata(tmp_path: Path) -> None:
    categories_dir = tmp_path / "categories"
    categories_dir.mkdir()
    (categories_dir / "book.yaml").write_text(
        """
category_name: book
display_name: Book Radar
sources:
  - name: Bookstore Ranking
    type: rss
    url: https://example.com/feed.xml
    language: ko
    country: KR
    trust_tier: T1_official
    collection_tier: C1_rss
    content_type: bestseller
    config:
      event_model: sales_ranking
entities:
  - name: BookType
    display_name: Book Type
    keywords:
      - bestseller
""",
        encoding="utf-8",
    )

    cfg = load_category_config("book", categories_dir=categories_dir)

    assert cfg.sources[0].language == "ko"
    assert cfg.sources[0].country == "KR"
    assert cfg.sources[0].trust_tier == "T1_official"
    assert cfg.sources[0].collection_tier == "C1_rss"
    assert cfg.sources[0].content_type == "bestseller"
    assert cfg.sources[0].config == {"event_model": "sales_ranking"}


def test_load_category_quality_config_preserves_quality_overlay(tmp_path: Path) -> None:
    categories_dir = tmp_path / "categories"
    categories_dir.mkdir()
    (categories_dir / "book.yaml").write_text(
        """
category_name: book
data_quality:
  priority: P2
  quality_outputs:
    tracked_event_models:
      - sales_ranking
source_backlog:
  operational_candidates:
    - id: library_bigdata_lending
""",
        encoding="utf-8",
    )

    quality = load_category_quality_config("book", categories_dir=categories_dir)

    assert quality["data_quality"]["priority"] == "P2"
    assert quality["source_backlog"]["operational_candidates"][0]["id"] == (
        "library_bigdata_lending"
    )
