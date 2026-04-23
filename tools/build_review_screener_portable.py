from __future__ import annotations

import shutil
import sys
import textwrap
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from version import get_version


APP_NAME = "KHQuantReviewScreener"


def ensure_pyinstaller():
    try:
        from PyInstaller.__main__ import run as pyinstaller_run
    except ImportError as exc:
        raise SystemExit(
            "缺少 PyInstaller。请先运行 .\\.venv311\\Scripts\\python -m pip install pyinstaller"
        ) from exc
    return pyinstaller_run


def safe_rmtree(path: Path):
    if path.exists():
        shutil.rmtree(path)


def copy_tree(src: Path, dst: Path):
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def write_portable_readme(target_dir: Path, version: str):
    content = textwrap.dedent(
        f"""\
        KHQuant Review Screener Portable v{version}

        运行方式
        - 直接双击 KHQuantReviewScreener.exe

        数据目录
        - cache\\market_data.duckdb: 本地数据库
        - reports\\: 筛选报告
        - data\\: 内置股票池数据

        注意事项
        - 首次同步数据时会在当前目录生成 cache 和 reports
        - 请保持整个解压目录结构完整，不要只移动 exe
        - 同一份目录请只打开一个程序实例
        """
    )
    (target_dir / "README_PORTABLE.txt").write_text(content, encoding="utf-8")


def make_zip(source_dir: Path, zip_path: Path):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for file_path in source_dir.rglob("*"):
            arcname = Path(source_dir.name) / file_path.relative_to(source_dir)
            archive.write(file_path, arcname)


def main():
    pyinstaller_run = ensure_pyinstaller()
    root = ROOT
    version = get_version()
    package_name = f"{APP_NAME}-v{version}-windows-portable"

    build_root = root / "build" / "review_screener_portable"
    dist_root = build_root / "dist"
    work_root = build_root / "work"
    spec_root = build_root / "spec"
    bundle_root = dist_root / APP_NAME
    release_root = root / "release"
    portable_dir = release_root / package_name
    zip_path = release_root / f"{package_name}.zip"

    safe_rmtree(build_root)
    safe_rmtree(portable_dir)
    release_root.mkdir(parents=True, exist_ok=True)

    args = [
        "--noconfirm",
        "--clean",
        "--windowed",
        "--name",
        APP_NAME,
        "--icon",
        str(root / "icons" / "stock_icon.ico"),
        "--distpath",
        str(dist_root),
        "--workpath",
        str(work_root),
        "--specpath",
        str(spec_root),
        "--paths",
        str(root),
        "--hidden-import",
        "matplotlib.backends.backend_qt5agg",
        "--collect-data",
        "matplotlib",
        "--collect-submodules",
        "matplotlib.backends",
        "--collect-all",
        "duckdb",
        str(root / "review_screener_portable_main.py"),
    ]

    pyinstaller_run(args)

    copy_tree(bundle_root, portable_dir)
    copy_tree(root / "data", portable_dir / "data")
    copy_tree(root / "icons", portable_dir / "icons")
    (portable_dir / "cache").mkdir(parents=True, exist_ok=True)
    (portable_dir / "reports").mkdir(parents=True, exist_ok=True)
    write_portable_readme(portable_dir, version)
    make_zip(portable_dir, zip_path)

    print(f"Portable folder: {portable_dir}")
    print(f"Portable zip: {zip_path}")


if __name__ == "__main__":
    main()
