"""linkbase・スキーマの解析。"""

import pytest

from edinet_xbrl_prep import linkbase
from tests import samples

pytestmark = pytest.mark.target("linkbase")


@pytest.mark.target("linkbase.format_taxonomi")
@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF",
         "jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF"),
        ("jpcrp_030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxes",
         "jpcrp_030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxes"),
        ("prefix_value", "prefix:value"),
    ],
)
def test_format_taxonomi_replaces_last_underscore(text, expected):
    """最後の「_」だけを「:」にする（旧 tests/test_utils.py の3件）。"""
    assert linkbase.format_taxonomi(text) == expected


@pytest.mark.target("linkbase.format_taxonomi")
@pytest.mark.parametrize(("text", "message"), [("", "入力文字列が空"), ("NoUnderscore", "アンダースコア")])
def test_format_taxonomi_rejects_invalid(text, message):
    """空文字・「_」のない文字列は ValueError（旧 tests/test_utils.py の2件）。"""
    with pytest.raises(ValueError, match=message):
        linkbase.format_taxonomi(text)


@pytest.mark.target("linkbase.key_from_href", "linkbase.role_suffix", "linkbase.label_role_name")
def test_names():
    """href → key、ロール URI → ロール名、ラベルの種類 URI → 名前。"""
    assert linkbase.key_from_href("jppfs_cor_2023-12-01.xsd#jppfs_cor_Assets") == "jppfs_cor:Assets"
    assert linkbase.role_suffix("http://x/role/jppfs/rol_BalanceSheet") == "rol_BalanceSheet"
    assert linkbase.label_role_name("http://www.xbrl.org/2003/role/verboseLabel") == "verboseLabel"
    assert (
        linkbase.label_role_name("http://disclosure.edinet-fsa.go.jp/jppfs/cns/role/label")
        == "jppfs/cns/role/label"
    )


@pytest.mark.target("linkbase.parse_presentation")
def test_parse_presentation_keeps_order_and_preferred_label():
    """親子・order・preferredLabel（種類の名前だけ）を読む。"""
    data = samples.presentation(
        [("jppfs_cor_Abstract", "jppfs_cor_Assets", 2.0, "totalLabel"),
         ("jppfs_cor_Abstract", "jppfs_cor_CashAndDeposits", 1.0, None)]
    )
    frame = linkbase.parse_presentation(data)
    assert list(frame.columns) == linkbase.PRESENTATION_COLUMNS
    assert frame.role.unique().tolist() == ["rol_ConsolidatedBalanceSheet"]
    assets = frame[frame.child_key == "jppfs_cor:Assets"].iloc[0]
    assert (assets.parent_key, assets.order, assets.preferred_label) == ("jppfs_cor:Abstract", 2.0, "totalLabel")
    assert frame[frame.child_key == "jppfs_cor:CashAndDeposits"].preferred_label.isna().all()


@pytest.mark.target("linkbase.parse_calculation")
def test_parse_calculation_weight_and_duplicates():
    """weight を読み、同じロール・同じ親子の重複は最初の1つを残す。"""
    data = samples.calculation(
        [("jppfs_cor_Assets", "jppfs_cor_CurrentAssets", 1),
         ("jppfs_cor_Assets", "jppfs_cor_AllowanceForDoubtfulAccounts", -1),
         ("jppfs_cor_Assets", "jppfs_cor_CurrentAssets", 1)]
    )
    frame = linkbase.parse_calculation(data)
    assert len(frame) == 2
    assert frame.set_index("child_key").weight.to_dict() == {
        "jppfs_cor:CurrentAssets": 1.0, "jppfs_cor:AllowanceForDoubtfulAccounts": -1.0
    }


@pytest.mark.target("linkbase.parse_labels")
def test_parse_labels_uses_own_locator():
    """ラベルファイル自身の locator で科目を決める（旧版は presentation の参照名で引き直していた）。"""
    data = samples.labels(
        [("jpcrp030000-asr_E00001-000_OriginalItem", samples.LABEL, "ja", "独自科目"),
         ("jpcrp030000-asr_E00001-000_OriginalItem", samples.VERBOSE, "ja", "独自科目（冗長）"),
         ("jppfs_cor_Assets", samples.LABEL, "en", "Assets")],
        xsd="jpcrp030000-asr-001_E00001-000.xsd",
    )
    frame = linkbase.parse_labels(data)
    assert set(map(tuple, frame[["key", "label_role", "lang"]].values)) == {
        ("jpcrp030000-asr_E00001-000:OriginalItem", "label", "ja"),
        ("jpcrp030000-asr_E00001-000:OriginalItem", "verboseLabel", "ja"),
        ("jppfs_cor:Assets", "label", "en"),
    }


@pytest.mark.target("linkbase.schema_imports")
def test_schema_imports_reads_edinet_versions_only():
    """EDINET タクソノミの import だけを、モジュール → 版の日付で返す。"""
    data = samples.schema({"jppfs": "2023-12-01", "jpcrp": "2023-12-01"})
    assert linkbase.schema_imports(data) == {"jppfs": "2023-12-01", "jpcrp": "2023-12-01"}
