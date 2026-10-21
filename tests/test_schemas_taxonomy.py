"""スキーマの検査と、タクソノミのキャッシュ。"""

import pandas as pd
import pandera.pandas as pa
import pytest

from edinet_xbrl_prep.schemas import FsTblDf, ProcessingLogDf
from edinet_xbrl_prep.taxonomy import TaxonomyCache, seed_from_taxonomy_zip
from tests import samples
from tests.conftest import write_zip


def _fs_row(**overrides):
    row = {c: None for c in FsTblDf.to_schema().columns}
    row.update(
        docid="S1000001", statement="BS", role="rol_BalanceSheet", consolidated_role=0, key="jppfs_cor:Cash",
        element_name="Cash", context_ref="CurrentYearInstant_NonConsolidatedMember", is_nil=0, is_text_block=0,
        is_abstract=0, n_source_facts=1, order=1, order_path="1", layer=0, parent_key_list=[],
        non_consolidated_flg=1, current_flg=1, prior_flg=0, calculated_flg=0, net_flg=0, net_src_flg=0,
        debit_flg=1, credit_flg=0,
    )
    row.update(overrides)
    return row


@pytest.mark.target("schemas.FsTblDf.unique_row", "schemas.FsTblDf.net_implies_calculated")
def test_fs_tbl_schema_checks():
    """（書類, ロール, 科目, context）の重複と、計算関係の親でない net_flg を拒否する。"""
    FsTblDf.validate(pd.DataFrame([_fs_row()]))
    with pytest.raises(pa.errors.SchemaError, match="unique_row"):
        FsTblDf.validate(pd.DataFrame([_fs_row(), _fs_row()]))
    with pytest.raises(pa.errors.SchemaError, match="net_implies_calculated"):
        FsTblDf.validate(pd.DataFrame([_fs_row(net_flg=1)]))
    with pytest.raises(pa.errors.SchemaError):
        FsTblDf.validate(pd.DataFrame([_fs_row(period_end="2024/03/31")]))


@pytest.mark.target("schemas.FsTblDf")
def test_fs_tbl_schema_is_strict():
    """定義にない列は拒否する（大福帳は FsBigTableDf で列を足す）。"""
    with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors), match="extra"):
        FsTblDf.validate(pd.DataFrame([_fs_row(extra="x")]))


@pytest.mark.target("schemas.ProcessingLogDf")
def test_processing_log_status():
    with pytest.raises(pa.errors.SchemaError):
        ProcessingLogDf.validate(pd.DataFrame([{"docid": "S1", "zip_path": "x", "status": "ok", "n_rows": 0,
                                                "accounting_standards": None, "taxonomy_versions": None, "message": None}]))


@pytest.mark.target("taxonomy.seed_from_taxonomy_zip", "taxonomy.TaxonomyCache.label_files", "taxonomy.TaxonomyCache.versions")
def test_seed_from_taxonomy_zip(tmp_path):
    """金融庁の zip のタクソノミを Arelle のキャッシュと同じ配置に置く。サンプルは置かず、既存のファイルは上書きしない。"""
    lab = samples.labels([("jppfs_cor_Cash", samples.LABEL, "ja", "現金")])
    zip_path = write_zip(tmp_path / "taxonomy_2099.zip", {
        "ALL_20991101/taxonomy/jppfs/2099-11-01/label/jppfs_2099-11-01_lab.xml": lab,
        "ALL_20991101/taxonomy/jppfs/2099-11-01/jppfs_cor_2099-11-01.xsd": "<xsd/>",
        "ALL_20991101/samples/2099-11-01/sample.xbrl": "sample",
    })
    cache = TaxonomyCache(tmp_path / "cache")
    placed = seed_from_taxonomy_zip(zip_path, cache)
    assert placed.placed.tolist() == [True, True]
    assert cache.versions("jppfs") == ["2099-11-01"]
    assert [f.name for f in cache.label_files("jppfs", "2099-11-01")] == ["jppfs_2099-11-01_lab.xml"]
    assert cache.common_labels("jppfs", "2099-11-01").text.tolist() == ["現金"]
    assert seed_from_taxonomy_zip(zip_path, cache).placed.tolist() == [False, False]


@pytest.mark.target("taxonomy.TaxonomyCache.controller")
def test_offline_controller(tmp_path):
    """Arelle のコントローラーは、このキャッシュを読み、offline ならネットワークに出ない。"""
    controller = TaxonomyCache(tmp_path / "cache", offline=True).controller(tmp_path / "arelle.log")
    try:
        assert controller.webCache.cacheDir == str((tmp_path / "cache").resolve())
        assert controller.webCache.workOffline is True
    finally:
        controller.close()


@pytest.mark.target("taxonomy.download_fsa_taxonomy")
def test_download_fsa_taxonomy(tmp_path, monkeypatch):
    """金融庁の zip を taxonomy_{年度}.zip に保存する（途中のファイルは .part）。すでにあれば取得しない。"""
    import edinet_xbrl_prep.taxonomy as taxonomy

    urls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, size):
            yield b"PK"

    monkeypatch.setattr(taxonomy.requests, "get", lambda url, **k: urls.append(url) or Response())
    path = taxonomy.download_fsa_taxonomy("2024", tmp_path)
    assert path.name == "taxonomy_2024.zip" and path.read_bytes() == b"PK"
    assert urls == [taxonomy.FSA_TAXONOMY_URLS["2024"]]
    taxonomy.download_fsa_taxonomy("2024", tmp_path)
    assert len(urls) == 1
