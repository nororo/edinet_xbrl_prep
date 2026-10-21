"""ZIP からのファイルの選び方と、ラベルの決め方。"""

import pandas as pd
import pytest

from edinet_xbrl_prep.archive import XbrlArchive, XbrlArchiveError
from edinet_xbrl_prep.taxonomy import (
    TaxonomyCache,
    company_display_labels,
    resolve_labels,
    wide_labels,
)
from tests import samples
from tests.conftest import write_zip

ASR = "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2024-03-31_01_2024-06-28"


@pytest.mark.target("archive.XbrlArchive.open")
def test_archive_selects_public_asr_files(tmp_path):
    """PublicDoc の本体（asr）のファイルだけを選ぶ。AuditDoc の lab.xml は拾わない（旧版は拾うことがあった）。"""
    path = write_zip(
        tmp_path / "S1000001.zip",
        {
            "XBRL/AuditDoc/jpaud-aai-cc-001_E00001-000_lab.xml": "audit",
            f"{ASR}.xbrl": "instance",
            f"{ASR}.xsd": "schema",
            f"{ASR}_lab.xml": "label",
            f"{ASR}_lab-en.xml": "label_en",
            f"{ASR}_pre.xml": "pre",
        },
    )
    archive = XbrlArchive.open(path)
    assert archive.get("label") == b"label"
    assert archive.get("label_en") == b"label_en"
    assert archive.get("calculation") is None
    instance = archive.extract(tmp_path / "work")
    assert instance.name.endswith(".xbrl") and instance.read_bytes() == b"instance"


@pytest.mark.target("archive.XbrlArchive.open")
def test_archive_errors(tmp_path):
    """本体のインスタンスがない ZIP・壊れた ZIP は XbrlArchiveError。"""
    no_instance = write_zip(tmp_path / "a.zip", {f"{ASR}.xsd": "schema"})
    with pytest.raises(XbrlArchiveError, match="インスタンス"):
        XbrlArchive.open(no_instance)
    broken = tmp_path / "b.zip"
    broken.write_bytes(b"not a zip")
    with pytest.raises(XbrlArchiveError, match="開けない"):
        XbrlArchive.open(broken)


def _labels(rows):
    return pd.DataFrame(rows, columns=["key", "label_role", "lang", "text"])


@pytest.mark.target("taxonomy.wide_labels")
def test_wide_labels_one_row_per_key():
    """1科目1行。日本語・英語 × 標準・冗長の4列。"""
    wide = wide_labels(_labels([
        ("a:X", "label", "ja", "現金"), ("a:X", "verboseLabel", "ja", "現金（冗長）"),
        ("a:X", "label", "en", "Cash"), ("a:X", "terseLabel", "ja", "使わない"),
    ]))
    row = wide.loc["a:X"]
    assert (row.label_jp, row.label_jp_long, row.label_en) == ("現金", "現金（冗長）", "Cash")
    assert pd.isna(row.label_en_long)
    assert list(wide.columns) == ["label_jp", "label_jp_long", "label_en", "label_en_long"]


@pytest.mark.target("taxonomy.company_display_labels")
def test_company_industry_label_becomes_standard():
    """企業が業種別の種類で書いた表示名は、標準のラベルがなければ標準として使う。"""
    out = company_display_labels(_labels([
        ("jppfs_cor:BuildingsNet", "jppfs/cte/role/label", "ja", "建物"),
        ("jppfs_cor:Land", "jppfs/cte/role/label", "ja", "業種別"), ("jppfs_cor:Land", "label", "ja", "土地"),
    ]))
    assert set(map(tuple, out[out.label_role == "label"][["key", "text"]].values)) == {
        ("jppfs_cor:BuildingsNet", "建物"), ("jppfs_cor:Land", "土地")
    }


@pytest.mark.target("taxonomy.resolve_labels", "taxonomy.TaxonomyCache.common_labels")
def test_resolve_labels_company_first_then_common(tmp_path):
    """企業別のラベルを優先し、ない列は書類が import する版の共通ラベルで補う。版のないモジュールは missing。"""
    cache = TaxonomyCache(tmp_path / "cache")
    base = cache.edinet_root / "jppfs" / "2099-01-01" / "label"
    base.mkdir(parents=True)
    (base / "jppfs_2099-01-01_lab.xml").write_bytes(samples.labels([
        ("jppfs_cor_Assets", samples.LABEL, "ja", "資産合計"),
        ("jppfs_cor_Cash", samples.LABEL, "ja", "現金"),
    ]))
    (base / "jppfs_2099-01-01_lab-en.xml").write_bytes(samples.labels([
        ("jppfs_cor_Cash", samples.LABEL, "en", "Cash"),
    ]))
    company = _labels([("jppfs_cor:Cash", "label", "ja", "現金及び預金")])
    labels, missing = resolve_labels(company, {"jppfs": "2099-01-01", "jpcrp": "2099-01-01"}, cache)
    assert labels.loc["jppfs_cor:Cash", "label_jp"] == "現金及び預金"
    assert labels.loc["jppfs_cor:Cash", "label_en"] == "Cash"
    assert labels.loc["jppfs_cor:Cash", "label_source"] == "company"
    assert labels.loc["jppfs_cor:Assets", "label_source"] == "common"
    assert missing == ["jpcrp/2099-01-01"]
    assert labels.index.is_unique
