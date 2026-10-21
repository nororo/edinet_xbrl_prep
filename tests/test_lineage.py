"""表の保存とデータリネージ。"""

import json

import pandas as pd
import pytest

from edinet_xbrl_prep import lineage
from edinet_xbrl_prep.schemas import ProcessingLogDf


def _save(path, data):
    return lineage.save_table(
        data,
        path,
        schema=ProcessingLogDf,
        purpose="テスト",
        source_code=_save,
        source_data_list=["入力A"],
        data_structure="1行＝1書類",
        importance_columns={"docid": "書類管理番号"},
        run_config={"statements": ["BS"]},
    )


LOG = pd.DataFrame(
    [{"docid": "S1000001", "zip_path": "x.zip", "status": "success", "n_rows": 3,
      "accounting_standards": "Japan GAAP", "taxonomy_versions": None, "message": "あ" * 500}]
)


@pytest.mark.target("lineage.save_table", "lineage.build_lineage", "lineage.read_table")
@pytest.mark.parametrize("suffix", [".parquet", ".csv", ".jsonl"])
def test_save_table_writes_lineage(tmp_path, suffix):
    """保存した表と同じ場所に {名前}_lin.json を書く。必須10項目がそろい、行数・列名・1行目が表と一致する。"""
    path = tmp_path / f"log{suffix}"
    lin_path = _save(path, LOG)
    assert lin_path == tmp_path / "log_lin.json"
    lin = json.loads(lin_path.read_text(encoding="utf-8"))
    assert set(lineage.REQUIRED_KEYS) <= set(lin)
    assert lin["file_name"] == str(path.resolve())
    assert lin["source_code"].endswith("test_lineage.py::_save")
    assert lin["record_num"] == 1 and lin["schema"] == list(LOG.columns)
    assert lin["record_example"]["docid"] == "S1000001"
    assert len(lin["record_example"]["message"]) == lineage.EXAMPLE_MAX_CHARS + 1  # 長い文字列は切る
    assert len(lin["sha256"]) == 64 and lin["config"] == {"statements": ["BS"]}
    assert lineage.read_table(path).docid.tolist() == ["S1000001"]


@pytest.mark.target("lineage.save_table")
def test_save_table_validates_schema(tmp_path):
    """スキーマを満たさない表は保存しない。"""
    with pytest.raises(Exception, match="status"):
        _save(tmp_path / "bad.parquet", LOG.assign(status="unknown"))
    assert not (tmp_path / "bad.parquet").exists()


@pytest.mark.target("lineage.validate_lineage")
def test_validate_lineage():
    """必須項目の欠け・空・行数の型を検査する。"""
    good = {k: "x" for k in lineage.REQUIRED_KEYS} | {"record_num": 0, "source_data_list": ["a"], "record_example": {}}
    lineage.validate_lineage(good)
    with pytest.raises(lineage.LineageError, match="必須項目がない"):
        lineage.validate_lineage({k: v for k, v in good.items() if k != "purpose"})
    with pytest.raises(lineage.LineageError, match="空"):
        lineage.validate_lineage(good | {"source_data_list": []})
    with pytest.raises(lineage.LineageError, match="record_num"):
        lineage.validate_lineage(good | {"record_num": -1})


@pytest.mark.target("lineage.to_json_value")
def test_to_json_value():
    """numpy・pandas の値と欠損を JSON の値に直す。"""
    import numpy as np

    assert lineage.to_json_value({"a": np.int64(1), "b": float("nan"), "c": pd.NA, "d": np.array([1, 2])}) == {
        "a": 1, "b": None, "c": None, "d": [1, 2]
    }


@pytest.mark.target("lineage.describe_source_code", "lineage.source_code_date", "lineage.lineage_path", "lineage.package_version")
def test_source_code_description(tmp_path):
    """関数から「パッケージ/モジュール.py::関数名」と最終更新日を作る。文字列はそのまま。"""
    from edinet_xbrl_prep.fs_tbl import build_fs_table

    label, date = lineage.describe_source_code(build_fs_table)
    assert label == "edinet_xbrl_prep/fs_tbl.py::build_fs_table"
    assert len(date) == 10 and date[4] == "-"
    assert lineage.describe_source_code("手作業") == ("手作業", None)
    assert lineage.lineage_path(tmp_path / "a.parquet") == tmp_path / "a_lin.json"
    assert lineage.package_version() is not None
