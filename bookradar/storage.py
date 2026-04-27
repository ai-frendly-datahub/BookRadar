from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import cast

from radar_core.exceptions import StorageError
from radar_core.storage import RadarStorage as CoreRadarStorage

from .models import Article


def _utc_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


class RadarStorage(CoreRadarStorage):
    def recent_articles_by_collected_at(
        self,
        category: str,
        *,
        days: int = 7,
        limit: int = 200,
    ) -> list[Article]:
        since = _utc_naive(datetime.now(UTC) - timedelta(days=days))
        cur = self.conn.execute(
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
            WHERE category = ? AND collected_at >= ?
            ORDER BY collected_at DESC, link DESC
            LIMIT ?
            """,
            [category, since, limit],
        )
        rows = cast(
            list[
                tuple[
                    str,
                    str,
                    str,
                    str,
                    str | None,
                    datetime | None,
                    datetime | None,
                    str | None,
                    str | None,
                ]
            ],
            cur.fetchall(),
        )
        return [_article_from_row(row) for row in rows]


def _article_from_row(
    row: tuple[
        str,
        str,
        str,
        str,
        str | None,
        datetime | None,
        datetime | None,
        str | None,
        str | None,
    ],
) -> Article:
    (
        category_value,
        source,
        title,
        link,
        summary,
        published,
        collected_at,
        raw_entities,
        raw_ontology,
    ) = row

    entities: dict[str, list[str]] = {}
    if raw_entities:
        try:
            parsed = cast(object, json.loads(raw_entities))
            if isinstance(parsed, dict):
                for name, values in cast(dict[object, object], parsed).items():
                    if not isinstance(name, str) or not isinstance(values, list):
                        continue
                    entities[name] = [str(value) for value in cast(list[object], values)]
        except json.JSONDecodeError:
            entities = {}

    ontology: dict[str, object] = {}
    if raw_ontology:
        try:
            parsed = cast(object, json.loads(raw_ontology))
            if isinstance(parsed, dict):
                ontology = {
                    str(name): value
                    for name, value in cast(dict[object, object], parsed).items()
                    if str(name).strip()
                }
        except json.JSONDecodeError:
            ontology = {}

    return Article(
        title=str(title),
        link=str(link),
        summary=str(summary) if summary is not None else "",
        published=published if isinstance(published, datetime) else None,
        source=str(source),
        category=str(category_value),
        matched_entities=entities,
        collected_at=collected_at if isinstance(collected_at, datetime) else None,
        ontology=ontology,
    )


__all__ = ["RadarStorage", "StorageError"]
