"""EDINET taxonomy cache, Arelle reading that cache, and resolution of account labels.

One directory (``TaxonomyCache.cache_dir``) serves as Arelle's web cache. The common labels are read from
the same directory for the version dates imported by each document.

- By default (``offline=False``), Arelle fetches files missing from the cache from EDINET
  (disclosure.edinet-fsa.go.jp) and others and stores them in the cache. Later runs read the cache.
- With ``offline=True``, nothing is fetched and only the cache is read (to fix the inputs for reproducibility).
  The cache is one built by an earlier run, or one built from an FSA zip with ``seed_from_taxonomy_zip``.
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

# EDINET taxonomy zips (by fiscal year) published by the FSA.
# Add 2026 (2025-11-01) and later after checking the URL (or pass ``url`` to download_fsa_taxonomy).
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
    """The label files of the common taxonomy are not in the cache."""


@dataclass(frozen=True)
class TaxonomyCache:
    """Taxonomy cache read by Arelle and for the common labels."""

    cache_dir: Path
    offline: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "cache_dir", Path(self.cache_dir).expanduser().resolve())
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def edinet_root(self) -> Path:
        return self.cache_dir / "http" / EDINET_HOST / "taxonomy"

    def controller(self, log_file: Path):
        """Arelle controller that reads this cache."""
        from arelle import Cntlr

        controller = Cntlr.Cntlr(logFileName=str(log_file))
        controller.webCache.cacheDir = str(self.cache_dir)
        controller.webCache.workOffline = self.offline
        return controller

    def label_files(self, module: str, version: str) -> list[Path]:
        """Label files (Japanese and English) of one module and version of the common taxonomy."""
        base = self.edinet_root / module / version / "label"
        return sorted(base.glob(f"{module}_*_lab.xml")) + sorted(base.glob(f"{module}_*_lab-en.xml"))

    def common_labels(self, module: str, version: str) -> pd.DataFrame:
        """Common taxonomy labels (columns key, label_role, lang, text), cached in the process."""
        return _common_labels(str(self.cache_dir), module, version)

    def versions(self, module: str = "jpcrp") -> list[str]:
        """Version dates of a module in the cache."""
        base = self.edinet_root / module
        return sorted(p.name for p in base.iterdir() if p.is_dir()) if base.exists() else []


@cache
def _common_labels(cache_dir: str, module: str, version: str) -> pd.DataFrame:
    files = TaxonomyCache(Path(cache_dir)).label_files(module, version)
    if not files:
        raise TaxonomyLabelsMissing(f"ラベルファイルがない: {module} {version}（{cache_dir}）")
    return pd.concat([parse_labels(f.read_bytes()) for f in files], ignore_index=True)


def download_fsa_taxonomy(year: str, dest_dir: Path, *, url: str | None = None) -> Path:
    """Download an FSA taxonomy zip (does nothing if it already exists)."""
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
    """Extract an FSA taxonomy zip into the Arelle cache layout and return the list of files.

    A path in the zip (``{folder}/taxonomy/jpcrp/...``) is placed at ``http/disclosure.edinet-fsa.go.jp/taxonomy/jpcrp/...``.
    Existing files are not overwritten (``placed`` is False in the list).
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


# ---------------------------------------------------------------- labels
# Determine one row of labels (Japanese/English, standard/verbose) per account.
#
# Sources in order of priority:
# 1. Company-specific labels of the document (``_lab.xml`` and ``_lab-en.xml`` in the ZIP)
# 2. Common taxonomy labels (jpcrp, jppfs and jpigp of the versions imported by the document, read from ``TaxonomyCache``)

LABEL_MODULES = ("jpcrp", "jppfs", "jpigp")
LABEL_COLUMNS = {
    ("ja", "label"): "label_jp",
    ("ja", "verboseLabel"): "label_jp_long",
    ("en", "label"): "label_en",
    ("en", "verboseLabel"): "label_en_long",
}
STANDARD_ROLES = {"label", "verboseLabel"}


def wide_labels(labels: pd.DataFrame) -> pd.DataFrame:
    """Long-format labels -> one row per account (index = key, columns label_jp, label_jp_long, label_en, label_en_long)."""
    columns = list(LABEL_COLUMNS.values())
    if labels.empty:
        return pd.DataFrame(columns=columns, index=pd.Index([], name="key"), dtype=object)
    labels = labels.assign(column=[
        LABEL_COLUMNS.get((str(lang)[:2], role))
        for lang, role in zip(labels.lang, labels.label_role, strict=True)
    ]).dropna(subset=["column"])
    # If an account has several labels of the same kind (usually not), take the first in string order
    labels = labels.sort_values(["key", "column", "text"]).drop_duplicates(["key", "column"])
    return labels.pivot(index="key", columns="column", values="text").reindex(columns=columns)


def company_display_labels(company_labels: pd.DataFrame) -> pd.DataFrame:
    """Treat company-specific labels written with an industry-specific role (``jppfs/cte/role/label``, etc.)
    as the standard role.

    Companies sometimes write their display name for a standard account with an industry-specific role. If the
    same account and language has no label of the standard role, that label is used as the standard one.
    Industry-specific labels of the common taxonomy are not used.
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
    """Return the label table for the accounts of a document and the list of missing common labels.

    The label table has index = key and columns label_jp, label_jp_long, label_en, label_en_long, label_source
    (``company``: at least one company-specific label was used, ``common``: common labels only).
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
    merged = company.combine_first(common)  # per column, the company-specific label if any, otherwise the common one
    source = pd.Series("common", index=merged.index)
    source[merged.index.isin(company.index)] = "company"
    return merged.assign(label_source=source), missing
