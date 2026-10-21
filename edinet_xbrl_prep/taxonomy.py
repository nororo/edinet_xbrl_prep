"""EDINET タクソノミのキャッシュ、そのキャッシュを読む Arelle、勘定科目のラベルの決定。

旧版は2つの経路でタクソノミを読んでいた。
1. ``account_list_common``：金融庁のタクソノミ zip を年度ごとにダウンロードし、共通ラベルを取り出す
2. Arelle：書類が参照するタクソノミを、利用者のホームの Arelle キャッシュ経由で取得する

ここでは1つのディレクトリ（``TaxonomyCache.cache_dir``）を Arelle のキャッシュとして使い、共通ラベルも
同じディレクトリから、書類が import する版の日付で読む。

- 既定（``offline=False``）では、キャッシュにないファイルを Arelle が EDINET（disclosure.edinet-fsa.go.jp）
  などから取得してキャッシュに置く。2回目以降はキャッシュを読む
- ``offline=True`` では、ネットワークに出ずキャッシュだけを読む（再現性を固定したいとき）。
  キャッシュは、前回の実行で作ったもの、または ``seed_from_taxonomy_zip`` で金融庁の zip から作る
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests

from edinet_xbrl_prep.linkbase import parse_labels

EDINET_HOST = "disclosure.edinet-fsa.go.jp"

# 金融庁が公開する EDINET タクソノミ（年度版）の zip。旧版 account_list_common._download_taxonomy の一覧に 2025 を加えた。
# 2026 年版（2025-11-01）以降は、URL を確かめてから追加する（または download_fsa_taxonomy に url を渡す）
FSA_TAXONOMY_URLS = {
    "2025": "https://www.fsa.go.jp/search/20241112/1c_Taxonomy.zip",
    "2024": "https://www.fsa.go.jp/search/20231211/1c_Taxonomy.zip",
    "2023": "https://www.fsa.go.jp/search/20221108/1c_Taxonomy.zip",
    "2022": "https://www.fsa.go.jp/search/20211109/1c_Taxonomy.zip",
    "2021": "https://www.fsa.go.jp/search/20201110/1c_Taxonomy.zip",
    "2020": "https://www.fsa.go.jp/search/20191101/1c_Taxonomy.zip",
    "2019": "https://www.fsa.go.jp/search/20190228/1c_Taxonomy.zip",
    "2018": "https://www.fsa.go.jp/search/20180228/1c_Taxonomy.zip",
    "2017": "https://www.fsa.go.jp/search/20170228/1c.zip",
    "2016": "https://www.fsa.go.jp/search/20160314/1c.zip",
    "2015": "https://www.fsa.go.jp/search/20150310/1c.zip",
    "2014": "https://www.fsa.go.jp/search/20140310/1c.zip",
}


class TaxonomyLabelsMissing(FileNotFoundError):
    """共通タクソノミのラベルファイルがキャッシュにない。"""


@dataclass(frozen=True)
class TaxonomyCache:
    """Arelle と共通ラベルが読むタクソノミのキャッシュ。"""

    cache_dir: Path
    offline: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "cache_dir", Path(self.cache_dir).expanduser().resolve())
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def edinet_root(self) -> Path:
        return self.cache_dir / "http" / EDINET_HOST / "taxonomy"

    def controller(self, log_file: Path):
        """このキャッシュを読む Arelle のコントローラー。"""
        from arelle import Cntlr

        controller = Cntlr.Cntlr(logFileName=str(log_file))
        controller.webCache.cacheDir = str(self.cache_dir)
        controller.webCache.workOffline = self.offline
        return controller

    def label_files(self, module: str, version: str) -> list[Path]:
        """共通タクソノミ1モジュール・1版のラベルファイル（日本語・英語）。"""
        base = self.edinet_root / module / version / "label"
        return sorted(base.glob(f"{module}_*_lab.xml")) + sorted(base.glob(f"{module}_*_lab-en.xml"))

    def common_labels(self, module: str, version: str) -> pd.DataFrame:
        """共通タクソノミのラベル（列 key, label_role, lang, text）。プロセス内でキャッシュする。"""
        return _common_labels(str(self.cache_dir), module, version)

    def versions(self, module: str = "jpcrp") -> list[str]:
        """キャッシュにあるモジュールの版の日付。"""
        base = self.edinet_root / module
        return sorted(p.name for p in base.iterdir() if p.is_dir()) if base.exists() else []


@cache
def _common_labels(cache_dir: str, module: str, version: str) -> pd.DataFrame:
    files = TaxonomyCache(Path(cache_dir)).label_files(module, version)
    if not files:
        raise TaxonomyLabelsMissing(f"ラベルファイルがない: {module} {version}（{cache_dir}）")
    return pd.concat([parse_labels(f.read_bytes()) for f in files], ignore_index=True)


def download_fsa_taxonomy(year: str, dest_dir: Path, *, url: str | None = None) -> Path:
    """金融庁のタクソノミ zip をダウンロードする（すでにあれば何もしない）。"""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"taxonomy_{year}.zip"
    if target.exists():
        return target
    url = url or FSA_TAXONOMY_URLS[year]
    with requests.get(url, stream=True, timeout=(20, 300)) as response:
        response.raise_for_status()
        partial = target.with_suffix(".part")
        with partial.open("wb") as f:
            for chunk in response.iter_content(1 << 20):
                f.write(chunk)
    partial.rename(target)
    return target


def seed_from_taxonomy_zip(zip_path: Path, cache: TaxonomyCache) -> pd.DataFrame:
    """金融庁のタクソノミ zip を展開し、Arelle のキャッシュと同じ配置で置く。置いたファイルの一覧を返す。

    zip 内のパス（``{フォルダ}/taxonomy/jpcrp/...``）を ``http/disclosure.edinet-fsa.go.jp/taxonomy/jpcrp/...``
    に置く。すでに同じパスにファイルがあれば上書きしない（一覧の ``placed`` が False）。
    """
    rows = []
    with ZipFile(zip_path) as zf:
        for info in zf.infolist():
            name = "/" + info.filename
            if "/taxonomy/" not in name or name.endswith("/") or "/samples/" in name:
                continue
            target = cache.edinet_root / name.split("/taxonomy/", 1)[1]
            data = zf.read(info)
            placed = not target.exists()
            if placed:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
            rows.append(
                {
                    "path": str(target.relative_to(cache.cache_dir)),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "source": Path(zip_path).name,
                    "placed": placed,
                }
            )
    return pd.DataFrame(rows, columns=["path", "sha256", "source", "placed"])


# ---------------------------------------------------------------- ラベル
# 勘定科目のラベル（日本語・英語、標準・冗長）を、1科目1行に決める。
#
# 取得元と優先順位:
# 1. 書類の企業別ラベル（ZIP 内の ``_lab.xml``・``_lab-en.xml``）
# 2. 共通タクソノミのラベル（書類が import する版の jpcrp・jppfs・jpigp。``TaxonomyCache`` から読む）
#
# 旧版（``linkbasefile.make_account_label``・``account_list_common.assign_common_label``）との違い:
# - 企業別の冗長ラベル・英語ラベルを捨てていた（``join()`` の戻り値を使っていなかった）
# - 共通ラベルを「年度」で選んでいた（利用者が年度を指定）。ここでは書類が import する版の日付で選ぶ
# - jpigp（IFRS）の共通ラベルを使っていなかった
# - 共通ラベルを presentation にある科目に限っていた。ここではラベルファイルの全科目を使う
# - 企業別と共通のラベルを縦に連結してから科目と結合していたため、両方にある科目が2行になった

LABEL_MODULES = ("jpcrp", "jppfs", "jpigp")
LABEL_COLUMNS = {
    ("ja", "label"): "label_jp",
    ("ja", "verboseLabel"): "label_jp_long",
    ("en", "label"): "label_en",
    ("en", "verboseLabel"): "label_en_long",
}
STANDARD_ROLES = {"label", "verboseLabel"}


def wide_labels(labels: pd.DataFrame) -> pd.DataFrame:
    """縦持ちのラベル → 1科目1行（index＝key、列 label_jp, label_jp_long, label_en, label_en_long）。"""
    columns = list(LABEL_COLUMNS.values())
    if labels.empty:
        return pd.DataFrame(columns=columns, index=pd.Index([], name="key"), dtype=object)
    labels = labels.assign(column=[
        LABEL_COLUMNS.get((str(lang)[:2], role))
        for lang, role in zip(labels.lang, labels.label_role, strict=True)
    ]).dropna(subset=["column"])
    # 同じ科目・同じ種類のラベルが複数あるとき（通常はない）は、文字列の昇順で最初のもの
    labels = labels.sort_values(["key", "column", "text"]).drop_duplicates(["key", "column"])
    return labels.pivot(index="key", columns="column", values="text").reindex(columns=columns)


def company_display_labels(company_labels: pd.DataFrame) -> pd.DataFrame:
    """企業別ラベルのうち業種別の種類（``jppfs/cte/role/label`` など）で書かれたものを、標準の種類に読み替える。

    企業は、標準の科目に付けた自社の表示名を業種別の種類で書くことがある。同じ科目・言語に標準の種類の
    ラベルがなければ、それを標準のラベルとして使う。共通タクソノミの業種別ラベルは使わない。
    """
    if company_labels.empty:
        return company_labels
    tail = company_labels.label_role.str.rsplit("/", n=1).str[-1]
    industry = ~company_labels.label_role.isin(STANDARD_ROLES) & tail.isin(STANDARD_ROLES)
    converted = company_labels[industry].assign(label_role=tail[industry])
    standard = company_labels[~industry]
    existing = set(zip(standard.key, standard.label_role, standard.lang, strict=True))
    keep = [
        item not in existing
        for item in zip(converted.key, converted.label_role, converted.lang, strict=True)
    ]
    return pd.concat([standard, converted[keep]], ignore_index=True)


def resolve_labels(
    company_labels: pd.DataFrame, imports: dict[str, str], taxonomy: TaxonomyCache
) -> tuple[pd.DataFrame, list[str]]:
    """書類の科目のラベル表と、見つからなかった共通ラベルの一覧を返す。

    ラベル表は index＝key、列 label_jp, label_jp_long, label_en, label_en_long, label_source
    （``company``：企業別のラベルを1つ以上使った、``common``：共通ラベルだけ）。
    """
    common_frames, missing = [], []
    for module, version in imports.items():
        if module not in LABEL_MODULES:
            continue
        try:
            common_frames.append(taxonomy.common_labels(module, version))
        except TaxonomyLabelsMissing:
            missing.append(f"{module}/{version}")
    common = wide_labels(pd.concat(common_frames, ignore_index=True) if common_frames else company_labels.iloc[0:0])
    company = wide_labels(company_display_labels(company_labels))
    merged = company.combine_first(common)  # 列ごとに、企業別があればそれ、なければ共通
    source = pd.Series("common", index=merged.index)
    source[merged.index.isin(company.index)] = "company"
    return merged.assign(label_source=source), missing
