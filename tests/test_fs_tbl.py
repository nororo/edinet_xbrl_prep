"""fs_tbl の組み立て（表示順・ロールの選び方・計算関係・借方貸方）。"""

import pandas as pd
import pytest

from edinet_xbrl_prep import fs_tbl
from edinet_xbrl_prep.facts import FACT_COLUMNS, merge_duplicate_facts
from edinet_xbrl_prep.linkbase import parse_calculation, parse_presentation
from edinet_xbrl_prep.schemas import FsTblDf
from edinet_xbrl_prep.taxonomy import wide_labels
from tests import samples


def _arcs(rows):
    return pd.DataFrame(rows, columns=["parent_key", "child_key", "order", "preferred_label"])


@pytest.mark.target("fs_tbl.build_role_tree")
def test_role_tree_orders_more_than_100_children():
    """子が100を超えても順序が衝突しない（旧版は 0.01 ** 階層 の符号化で衝突した）。"""
    rows = [("root", f"c{i:03d}", float(i), None) for i in range(1, 151)]
    rows.append(("c001", "g1", 1.0, None))
    tree = fs_tbl.build_role_tree(_arcs(rows))
    assert tree.order["root"] == 1 and tree.order["c001"] == 2 and tree.order["g1"] == 3
    assert tree.order["c150"] == 152
    assert len(set(tree.order.values())) == len(tree.order)
    assert tree.order_path["g1"] == "1_1_1" and tree.layer["g1"] == 2
    assert tree.parents["g1"] == ["c001"]


@pytest.mark.target("fs_tbl.build_role_tree")
def test_role_tree_follows_arc_order_and_ignores_cycles():
    """子は arc の order の順。循環があっても止まる。"""
    tree = fs_tbl.build_role_tree(_arcs([("r", "b", 2.0, None), ("r", "a", 1.0, "totalLabel"), ("a", "r", 1.0, None)]))
    assert tree.order["a"] < tree.order["b"]
    assert tree.preferred_label["a"] == "totalLabel"


@pytest.mark.target("fs_tbl.statement_of_role")
@pytest.mark.parametrize(
    ("role", "expected"),
    [
        ("rol_ConsolidatedBalanceSheet", "BS"),
        ("rol_ConsolidatedStatementOfFinancialPositionIFRS", "BS"),
        ("rol_ConsolidatedStatementOfProfitOrLossIFRS", "PL"),
        ("rol_StatementOfCashFlows-indirect", "CF"),
        ("rol_ConsolidatedStatementOfComprehensiveIncome", "CI"),
        ("rol_CabinetOfficeOrdinanceOnDisclosureOfCorporateInformationEtcFormNo3AnnualSecuritiesReport", "report"),
        ("rol_NotesConsolidatedFinancialStatements", "notes"),
        ("rol_SomethingElse", None),
    ],
)
def test_statement_of_role(role, expected):
    """ロール名から財務諸表の種類を決める（IFRS の連結財務諸表を含む）。"""
    assert fs_tbl.statement_of_role(role) == expected


@pytest.mark.target("fs_tbl.statement_of_role")
def test_statement_of_role_respects_selection():
    """選んでいない種類のロールは対象外。"""
    assert fs_tbl.statement_of_role("rol_BalanceSheet", ["PL"]) is None


CALC = pd.DataFrame(
    [
        ("rol_BalanceSheet", "jppfs_cor:Assets", "jppfs_cor:CurrentAssets", 1, 1.0),
        ("rol_BalanceSheet", "jppfs_cor:CurrentAssets", "jppfs_cor:Cash", 1, 1.0),
        ("rol_BalanceSheet", "jppfs_cor:CurrentAssets", "jppfs_cor:Allowance", 2, -1.0),
        ("rol_BalanceSheet", "jppfs_cor:LiabilitiesAndNetAssets", "jppfs_cor:Liabilities", 1, 1.0),
        ("rol_BalanceSheet", "jppfs_cor:BuildingsNet", "jppfs_cor:Buildings", 1, 1.0),
        ("rol_BalanceSheet", "jppfs_cor:BuildingsNet", "jppfs_cor:Depreciation", 2, -1.0),
        ("rol_StatementOfIncome", "jppfs_cor:ProfitLoss", "jppfs_cor:NetSales", 1, 1.0),
        ("rol_StatementOfIncome", "jppfs_cor:ProfitLoss", "jppfs_cor:CostOfSales", 2, -1.0),
    ],
    columns=["role", "parent_key", "child_key", "order", "weight"],
)


@pytest.mark.target("fs_tbl.calculation_effects")
def test_calculation_effects_signs():
    """root を増やす科目は +1、減らす科目は -1、届かない科目は 0。"""
    effects = fs_tbl.calculation_effects(CALC[CALC.role == "rol_BalanceSheet"], ["jppfs_cor:Assets"])
    assert effects["jppfs_cor:Cash"] == 1
    assert effects["jppfs_cor:Allowance"] == -1
    assert effects["jppfs_cor:Liabilities"] == 0


@pytest.mark.target("fs_tbl.debit_credit_keys")
def test_debit_credit_keys():
    """借方：資産の増加・利益の減少。貸方：負債純資産の増加・利益の増加。"""
    debit, credit = fs_tbl.debit_credit_keys(CALC)
    assert {"jppfs_cor:Cash", "jppfs_cor:CostOfSales"} <= debit
    assert {"jppfs_cor:Liabilities", "jppfs_cor:NetSales"} <= credit
    assert "jppfs_cor:Allowance" not in debit | credit


@pytest.mark.target("fs_tbl.calculation_columns")
def test_net_flags():
    """「Net」で終わる計算関係の親に net_flg、その子に net_src_flg（旧版の net_flg は常に 0 だった）。"""
    keys = pd.Series(["jppfs_cor:BuildingsNet", "jppfs_cor:Buildings", "jppfs_cor:Cash"])
    columns = fs_tbl.calculation_columns(CALC[CALC.role == "rol_BalanceSheet"], keys)
    assert columns["net_flg"].tolist() == [1, 0, 0]
    assert columns["net_src_flg"].tolist() == [0, 1, 0]
    assert columns["calculated_flg"].tolist() == [1, 0, 0]
    assert columns["calc_parent_key"].tolist()[1] == "jppfs_cor:BuildingsNet"


def _fact(key, context, value="1"):
    row = dict.fromkeys(FACT_COLUMNS)
    row.update(key=key, context_ref=context, data_str=value, element_name=key.split(":")[1],
               period_type="instant", is_text_block=0, is_abstract=0, is_nil=0,
               instant_date="2024-03-31", period_end="2024-03-31")
    return row


@pytest.mark.target("fs_tbl.assemble_fs_tbl")
def test_assemble_keeps_matching_consolidation_and_validates():
    """連結のロールには連結の値、個別のロールには個別の値だけを残す。結果は FsTblDf を満たす。"""
    pre = pd.concat([
        parse_presentation(samples.presentation([("jppfs_cor_Abstract", "jppfs_cor_Cash", 1.0, None)])),
        parse_presentation(samples.presentation(
            [("jppfs_cor_Abstract", "jppfs_cor_Cash", 1.0, None)],
            role="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_BalanceSheet")),
    ])
    calc = parse_calculation(samples.calculation([("jppfs_cor_Assets", "jppfs_cor_Cash", 1)]))
    facts = merge_duplicate_facts(pd.DataFrame(
        [_fact("jppfs_cor:Cash", "CurrentYearInstant", "100"),
         _fact("jppfs_cor:Cash", "CurrentYearInstant_NonConsolidatedMember", "40"),
         _fact("jppfs_cor:Cash", "Prior1YearInstant", "90")],
        columns=FACT_COLUMNS,
    ))
    labels = wide_labels(pd.DataFrame([("jppfs_cor:Cash", "label", "ja", "現金")],
                                      columns=["key", "label_role", "lang", "text"])).assign(label_source="common")
    table = fs_tbl.assemble_fs_tbl("S1000001", facts, pre, calc, labels, "Japan GAAP")
    FsTblDf.validate(table)
    cons = table[table.role == "rol_ConsolidatedBalanceSheet"]
    solo = table[table.role == "rol_BalanceSheet"]
    assert sorted(cons.data_str) == ["100", "90"] and set(cons.non_consolidated_flg) == {0}
    assert solo.data_str.tolist() == ["40"] and set(solo.non_consolidated_flg) == {1}
    assert set(table.current_flg[table.context_ref.str.startswith("Current")]) == {1}
    assert set(table.debit_flg) == {1} and set(table.label_jp) == {"現金"}


@pytest.mark.target("facts.merge_duplicate_facts")
def test_merge_duplicate_facts():
    """同じ（要素, context）の Fact：テキストはつなぎ、数値は最初を残し、元の数を n_source_facts に残す。"""
    text = [_fact("jpcrp_cor:RisksTextBlock", "FilingDateInstant", "前半"),
            _fact("jpcrp_cor:RisksTextBlock", "FilingDateInstant", "後半")]
    for row in text:
        row["is_text_block"] = 1
    facts = pd.DataFrame(text + [_fact("a:X", "C", "1"), _fact("a:X", "C", "2"), _fact("a:Y", "C", "3")],
                         columns=FACT_COLUMNS)
    merged = merge_duplicate_facts(facts).set_index("key")
    assert merged.loc["jpcrp_cor:RisksTextBlock", "data_str"] == "前半\n後半"
    assert merged.loc["a:X", "data_str"] == "1" and merged.loc["a:X", "n_source_facts"] == 2
    assert merged.loc["a:Y", "n_source_facts"] == 1
