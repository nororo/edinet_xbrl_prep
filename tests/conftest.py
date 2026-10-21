"""テストの共通設定。

実際の EDINET 書類を使うテスト（``fixture_zip``）は、次の環境変数で場所を指定する。
- ``EDINET_FIXTURE_DIR``：書類の ZIP（``{docid}.zip``）を置いたディレクトリ。ないときは飛ばす
- ``EDINET_TAXONOMY_CACHE``：タクソノミのキャッシュ（既定 ``tests/.cache/taxonomy``。初回は EDINET から取得する）
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from edinet_xbrl_prep.taxonomy import TaxonomyCache

TESTS_DIR = Path(__file__).resolve().parent


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    path = os.environ.get("EDINET_FIXTURE_DIR")
    if not path or not Path(path).is_dir():
        pytest.skip("EDINET_FIXTURE_DIR（書類の ZIP の場所）が指定されていない")
    return Path(path)


@pytest.fixture(scope="session")
def taxonomy() -> TaxonomyCache:
    path = os.environ.get("EDINET_TAXONOMY_CACHE", str(TESTS_DIR / ".cache" / "taxonomy"))
    return TaxonomyCache(Path(path))


def write_zip(path: Path, files: dict[str, bytes | str]) -> Path:
    """テスト用の ZIP を作る。"""
    from zipfile import ZipFile

    with ZipFile(path, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return path
