from __future__ import annotations

from pathlib import Path
from typing import cast

import yaml
from radar_core.models import (
    CategoryConfig,
    EmailConfig,
    EntityDefinition,
    RadarSettings,
    Source,
    StandardNotificationConfig,
    WebhookConfig,
)


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _read_yaml(path: Path) -> dict[str, object]:
    raw = cast(object, yaml.safe_load(path.read_text(encoding="utf-8")))
    if isinstance(raw, dict):
        return {str(k): v for k, v in cast(dict[object, object], raw).items()}
    return {}


def _str(d: dict[str, object], k: str, default: str = "") -> str:
    v = d.get(k)
    return v if isinstance(v, str) and v.strip() else default


def _bool(d: dict[str, object], k: str, default: bool) -> bool:
    v = d.get(k)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        lowered = v.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return default


def _float(d: dict[str, object], k: str, default: float) -> float:
    v = d.get(k)
    if isinstance(v, int | float):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip())
        except ValueError:
            return default
    return default


def _str_list(d: dict[str, object], k: str) -> list[str]:
    v = d.get(k)
    if isinstance(v, list):
        values = cast(list[object], v)
    elif isinstance(v, tuple | set):
        values = list(cast(tuple[object, ...] | set[object], v))
    elif isinstance(v, str) and v.strip():
        values = [v]
    else:
        values = []
    return [str(value).strip() for value in values if str(value).strip()]


def _dict(d: dict[str, object], k: str) -> dict[str, object]:
    v = d.get(k)
    if isinstance(v, dict):
        return {str(key): value for key, value in cast(dict[object, object], v).items()}
    return {}


def _path(val: str) -> Path:
    p = Path(val).expanduser()
    return p if p.is_absolute() else (_PROJECT_ROOT / p).resolve()


def load_settings(config_path: Path | None = None) -> RadarSettings:
    f = config_path or _PROJECT_ROOT / "config" / "config.yaml"
    if not f.exists():
        raise FileNotFoundError(f"Config file not found: {f}")
    raw = _read_yaml(f)
    return RadarSettings(
        database_path=_path(_str(raw, "database_path", "data/radar_data.duckdb")),
        report_dir=_path(_str(raw, "report_dir", "reports")),
        raw_data_dir=_path(_str(raw, "raw_data_dir", "data/raw")),
        search_db_path=_path(_str(raw, "search_db_path", "data/search_index.db")),
    )


def load_category_config(category_name: str, categories_dir: Path | None = None) -> CategoryConfig:
    base = categories_dir or _PROJECT_ROOT / "config" / "categories"
    f = Path(base) / f"{category_name}.yaml"
    if not f.exists():
        raise FileNotFoundError(f"Category config not found: {f}")
    raw = _read_yaml(f)
    sources = []
    for s in raw.get("sources") or []:
        if isinstance(s, dict):
            sd = {str(k): v for k, v in cast(dict[object, object], s).items()}
            sources.append(
                Source(
                    name=_str(sd, "name", "Unnamed"),
                    type=_str(sd, "type", "rss"),
                    url=_str(sd, "url"),
                    id=_str(sd, "id"),
                    enabled=_bool(sd, "enabled", True),
                    language=_str(sd, "language"),
                    country=_str(sd, "country"),
                    region=_str(sd, "region"),
                    trust_tier=_str(sd, "trust_tier", "T3_professional"),
                    weight=_float(sd, "weight", 1.0),
                    content_type=_str(sd, "content_type", "news"),
                    collection_tier=_str(sd, "collection_tier", "C1_rss"),
                    producer_role=_str(sd, "producer_role"),
                    info_purpose=_str_list(sd, "info_purpose"),
                    notes=_str(sd, "notes"),
                    config=_dict(sd, "config"),
                )
            )
    entities = []
    for e in raw.get("entities") or []:
        if isinstance(e, dict):
            ed = {str(k): v for k, v in cast(dict[object, object], e).items()}
            kw_raw = ed.get("keywords", [])
            kws = [
                str(k).strip()
                for k in (kw_raw if isinstance(kw_raw, list) else [])
                if str(k).strip()
            ]
            entities.append(
                EntityDefinition(
                    name=_str(ed, "name", "entity"),
                    display_name=_str(ed, "display_name", _str(ed, "name", "entity")),
                    keywords=kws,
                )
            )
    dn = _str(raw, "display_name") or _str(raw, "category_name") or category_name
    return CategoryConfig(
        category_name=_str(raw, "category_name", category_name),
        display_name=dn,
        sources=sources,
        entities=entities,
    )


def load_category_quality_config(
    category_name: str, categories_dir: Path | None = None
) -> dict[str, object]:
    base = categories_dir or _PROJECT_ROOT / "config" / "categories"
    f = Path(base) / f"{category_name}.yaml"
    if not f.exists():
        raise FileNotFoundError(f"Category config not found: {f}")
    raw = _read_yaml(f)
    return {
        "data_quality": _dict(raw, "data_quality"),
        "source_backlog": _dict(raw, "source_backlog"),
    }


def load_notification_config(config_path: Path | None = None) -> StandardNotificationConfig:
    f = config_path or _PROJECT_ROOT / "config" / "config.yaml"
    if not f.exists():
        f = _PROJECT_ROOT / "config" / "notifications.yaml"
    if not f.exists():
        return StandardNotificationConfig(enabled=False, channels=[])

    raw = _read_yaml(f)
    notifications = _dict(raw, "notifications") if "notifications" in raw else raw
    email_raw = _dict(notifications, "email")
    webhook_raw = _dict(notifications, "webhook")
    email_enabled = _bool(email_raw, "enabled", False)
    webhook_enabled = _bool(webhook_raw, "enabled", False)
    channels = _str_list(notifications, "channels")
    if not channels:
        channels = [
            name
            for name, enabled in (
                ("email", email_enabled),
                ("webhook", webhook_enabled),
            )
            if enabled
        ]

    return StandardNotificationConfig(
        enabled=_bool(notifications, "enabled", False),
        channels=channels,
        email=EmailConfig(
            enabled=email_enabled,
            smtp_host=_str(email_raw, "smtp_host"),
            smtp_port=int(_float(email_raw, "smtp_port", 587)),
            smtp_user=_str(email_raw, "smtp_user"),
            smtp_password=_str(email_raw, "smtp_password"),
            from_addr=_str(email_raw, "from_addr"),
            to_addrs=_str_list(email_raw, "to_addrs"),
        ),
        webhook=WebhookConfig(
            enabled=webhook_enabled,
            url=_str(webhook_raw, "url"),
            method=_str(webhook_raw, "method", "POST"),
            headers={str(k): str(v) for k, v in _dict(webhook_raw, "headers").items()},
        ),
    )


__all__ = [
    "load_category_config",
    "load_category_quality_config",
    "load_notification_config",
    "load_settings",
]
