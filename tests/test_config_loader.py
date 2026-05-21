from __future__ import annotations

from pathlib import Path

from bookradar.config_loader import (
    load_category_config,
    load_category_quality_config,
    load_notification_config,
)


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


def test_load_notification_config_reads_global_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
notifications:
  enabled: true
  email:
    enabled: true
    smtp_host: smtp.example.com
    smtp_port: 2525
    smtp_user: radar
    smtp_password: secret
    from_addr: radar@example.com
    to_addrs:
      - ops@example.com
  webhook:
    enabled: true
    url: https://example.com/hook
    method: POST
    headers:
      X-Radar: book
""",
        encoding="utf-8",
    )

    cfg = load_notification_config(config_path)

    assert cfg.enabled is True
    assert cfg.channels == ["email", "webhook"]
    assert cfg.email is not None
    assert cfg.email.smtp_host == "smtp.example.com"
    assert cfg.email.smtp_port == 2525
    assert cfg.email.to_addrs == ["ops@example.com"]
    assert cfg.webhook is not None
    assert cfg.webhook.url == "https://example.com/hook"
    assert cfg.webhook.headers == {"X-Radar": "book"}


def test_book_config_disables_sources_with_known_crawl_failures() -> None:
    cfg = load_category_config("book")
    sources = {source.name: source for source in cfg.sources}

    disabled_after_health_review = {
        "Daniel Greene",
        "merphy napier",
        "r/52book",
        "r/Fantasy",
        "r/books",
        "r/booksuggestions",
        "r/horrorlit",
        "r/literature",
        "r/scifi",
        "r/suggestmeabook",
        "Shelf Awareness",
    }

    for source_name in disabled_after_health_review:
        source = sources[source_name]
        assert source.enabled is False
        assert "Disabled 2026-" in source.notes
