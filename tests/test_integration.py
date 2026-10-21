"""実際の EDINET 書類を使うテスト（環境変数 EDINET_FIXTURE_DIR に ZIP の場所を指定する）。

書類は参考プロジェクト（202610_fs_dataset）の tests/fixtures と同じもの。
"""

import json

import pytest

from edinet_xbrl_prep import build_fs_table, parse_document, read_table
from edinet_xbrl_prep.lineage import REQUIRED_KEYS
from edinet_xbrl_prep.schemas import FsTblDf

pytestmark = pytest.mark.fixture_zip

JAPAN_GAAP = "S100V7BC"  # 日本基準・連結あり・企業固有の科目あり（2025年の書類）
NON_CONSOLIDATED = "S100TAWG"  # 日本基準・個別のみ
IFRS = "S100TG59"
OLD_TAXONOMY = "S10019FE"  # 2014年のタクソノミ


@pytest.fixture(scope="module")
def parsed(fixture_dir, taxonomy):
    return {d: parse_document(fixture_dir / f"{d}.zip", taxonomy) for d in (JAPAN_GAAP, NON_CONSOLIDATED, IFRS, OLD_TAXONOMY)}


@pytest.mark.target("fs_tbl.parse_document", "fs_tbl.get_fs_tbl", "facts.load_facts", "facts.context_dimensions", "taxonomy.resolve_labels", "archive.XbrlArchive.extract")
def test_parse_document_valid_and_labelled(parsed):
    """どの書類も FsTblDf を満たし、BS・PL の科目にはラベルが付く。"""
    for docid, result in parsed.items():
        FsTblDf.validate(result.fs_tbl)
        fs = result.fs_tbl[result.fs_tbl.statement.isin(["BS", "PL"])]
        assert len(fs) > 0, docid
        assert fs.label_jp.notna().mean() > 0.99, docid
        assert result.info["missing_common_labels"] == [], docid
    # 次元（株主資本等変動計算書の列など）を JSON で持つ
    assert parsed[JAPAN_GAAP].fs_tbl.dimensions.dropna().str.startswith("{").all()
    assert parsed[JAPAN_GAAP].fs_tbl.dimensions.notna().any()


@pytest.mark.target("fs_tbl.statement_of_role")
def test_ifrs_statements_included(parsed):
    """IFRS の書類は、IFRS の連結財政状態計算書・連結損益計算書のロールを含む（旧版は含まなかった）。"""
    roles = set(parsed[IFRS].fs_tbl.role)
    assert {"rol_ConsolidatedStatementOfFinancialPositionIFRS", "rol_ConsolidatedStatementOfProfitOrLossIFRS"} <= roles
    assert parsed[IFRS].info["accounting_standards"] == "IFRS"


@pytest.mark.target("facts.context_dates")
def test_period_dates_match_document(parsed, fixture_dir):
    """当期末の時点・期末日は、書類の決算日（インスタンスのファイル名の日付）と同じ（旧版の1日のずれがない）。"""
    import re

    from edinet_xbrl_prep.archive import XbrlArchive

    for docid, result in parsed.items():
        name = XbrlArchive.open(fixture_dir / f"{docid}.zip").names["instance"]
        year_end = re.search(r"_(\d{4}-\d{2}-\d{2})_\d{2}_", name).group(1)
        fs = result.fs_tbl
        instants = fs[fs.context_ref.str.startswith("CurrentYearInstant")].instant_date
        durations = fs[fs.context_ref.str.startswith("CurrentYearDuration")].period_end
        assert len(instants) > 0 and len(durations) > 0, docid
        assert set(instants) == {year_end}, docid
        assert set(durations) == {year_end}, docid


@pytest.mark.target("fs_tbl.assemble_fs_tbl")
def test_non_consolidated_only_document(parsed):
    """個別のみの書類は、個別のロール・個別の値だけになる。"""
    fs = parsed[NON_CONSOLIDATED].fs_tbl
    fs = fs[fs.statement.isin(["BS", "PL"])]
    assert set(fs.consolidated_role) == {0} and set(fs.non_consolidated_flg) == {1}


@pytest.mark.target("fs_tbl.build_fs_table", "lineage.save_table")
def test_build_fs_table_saves_table_log_and_lineage(fixture_dir, taxonomy, tmp_path):
    """大福帳と処理結果を保存し、それぞれにリネージを付ける。壊れた ZIP・ない ZIP は failure として記録して続ける。"""
    import shutil

    zip_dir = tmp_path / "zip"
    zip_dir.mkdir()
    shutil.copy(fixture_dir / f"{NON_CONSOLIDATED}.zip", zip_dir)
    (zip_dir / "S1BROKEN.zip").write_bytes(b"broken")
    import pandas as pd

    documents = pd.DataFrame({"docID": [NON_CONSOLIDATED], "filerName": ["テスト株式会社"], "sector_label_33": ["サービス業"]})
    result = build_fs_table(
        [NON_CONSOLIDATED, "S1BROKEN", "S1MISSIN"], zip_dir, taxonomy,
        statements=["BS", "PL", "report"], documents=documents, out_path=tmp_path / "out" / "fs_big_table.parquet",
    )
    assert result.log.set_index("docid").status.to_dict() == {
        NON_CONSOLIDATED: "success", "S1BROKEN": "failure", "S1MISSIN": "failure"
    }
    table = read_table(result.paths["table"])
    assert len(table) == len(result.table) > 0
    assert set(table.statement) <= {"BS", "PL", "report"}
    assert set(table.filerName) == {"テスト株式会社"}
    for name in ("fs_big_table_lin.json", "fs_big_table_log_lin.json"):
        lin = json.loads((tmp_path / "out" / name).read_text(encoding="utf-8"))
        assert set(REQUIRED_KEYS) <= set(lin)
    lin = json.loads((tmp_path / "out" / "fs_big_table_lin.json").read_text(encoding="utf-8"))
    assert lin["record_num"] == len(table) and lin["source_code"].endswith("fs_tbl.py::build_fs_table")
