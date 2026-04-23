from __future__ import annotations

import sys
from pathlib import Path


def is_frozen_app() -> bool:
    return bool(getattr(sys, "frozen", False))


def get_app_root() -> Path:
    if is_frozen_app():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def get_resource_root() -> Path:
    meipass = getattr(sys, "_MEIPASS", None)
    if is_frozen_app() and meipass:
        return Path(meipass)
    return get_app_root()


def resolve_data_dir(app_root: Path) -> Path:
    app_root = Path(app_root)
    candidates = [app_root / "data", get_resource_root() / "data"]
    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return candidate
    return app_root / "data"


def get_icon_path(icon_name: str) -> Path:
    candidates = [
        get_app_root() / "icons" / icon_name,
        get_resource_root() / "icons" / icon_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]
