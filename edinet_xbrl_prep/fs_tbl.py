"""書類ごとの表（fs_tbl）と、複数書類をまとめた大福帳（One Big Table）を作る。

fs_tbl の1行＝1書類×1ロール×1科目×1 context の Fact。presentation の表示順・階層・親科目、
calculation の計算関係、ラベル、借方／貸方の印、連結／個別・当期／前期の印を付ける。

旧版（``fs_tbl.get_fs_tbl``・``linkbasefile``・``account_link_tracer``）との違い:
- 表示順：``0.01 ** 階層`` で実数に符号化していたため、子が100を超えると順序が衝突した。
  ここでは深さ優先の通し番号（``order``）、親からの並び（``order_path``）、深さ（``layer``）で持つ
- ロールの選び方：旧版の ``role_keyward_list``（部分一致）を、財務諸表の種類（BS・PL など）で指定する形にした。
  IFRS の連結財務諸表のロールと、包括利益計算書（CI）を加えた
- 連結／個別のロールの判定：ロール名に ``Consolidated`` を含むか（旧版は ``_Consolidated`` で判定し、
  注記の連結ロール ``rol_NotesConsolidated...`` を個別と誤っていた）
- ラベルがない科目は欠損のまま（旧版は ``'-'`` を入れていた）
- 計算関係の印（``calculated_flg``・``net_flg`` など）と借方／貸方の印を加えた
"""

from __future__ import annotations

import tempfile
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from edinet_xbrl_prep.archive import XbrlArchive
from edinet_xbrl_prep.facts import load_facts
from edinet_xbrl_prep.lineage import save_table
from edinet_xbrl_prep.linkbase import (
    CALCULATION_COLUMNS,
    LABEL_COLUMNS,
    PRESENTATION_COLUMNS,
    parse_calculation,
    parse_labels,
    parse_presentation,
    schema_imports,
)
from edinet_xbrl_prep.schemas import FsBigTableDf, FsTblDf, ProcessingLogDf
from edinet_xbrl_prep.taxonomy import TaxonomyCache, resolve_labels

# 財務諸表の種類 → ロール名（例：rol_ConsolidatedBalanceSheet）に「含まれる」文字列。
# 旧版ノートブックの fs_dict に、IFRS の連結財務諸表のロールと包括利益計算書（CI）を加えた。
STATEMENT_ROLE_PATTERNS: dict[str, list[str]] = {
    "BS": ["_BalanceSheet", "_ConsolidatedBalanceSheet", "_ConsolidatedStatementOfFinancialPositionIFRS"],
    "PL": ["_StatementOfIncome", "_ConsolidatedStatementOfIncome", "_ConsolidatedStatementOfProfitOrLossIFRS"],
    "CF": ["_StatementOfCashFlows", "_ConsolidatedStatementOfCashFlows", "_ConsolidatedStatementOfCashFlowsIFRS"],
    "SS": [
        "_StatementOfChangesInEquity",
        "_ConsolidatedStatementOfChangesInEquity",
        "_StatementOfChangesInNetAssets",
        "_ConsolidatedStatementOfChangesInNetAssets",
        "_ConsolidatedStatementOfChangesInEquityIFRS",
    ],
    "CI": ["_ConsolidatedStatementOfComprehensiveIncome"],
    "report": ["_CabinetOfficeOrdinanceOnDisclosure"],
    "notes": ["_Notes"],
}
DEFAULT_STATEMENTS = ("BS", "PL", "CF", "SS", "CI", "report", "notes")

# 借方／貸方の判定に使う、計算関係のたどり先
ASSETS = ("jppfs_cor:Assets", "jpigp_cor:AssetsIFRS")
LIABILITIES_AND_NET_ASSETS = ("jppfs_cor:LiabilitiesAndNetAssets", "jpigp_cor:LiabilitiesAndEquityIFRS")
PROFIT_LOSS = ("jppfs_cor:ProfitLoss", "jpigp_cor:ProfitLossIFRS")

FS_COLUMNS = list(FsTblDf.to_schema().columns)


def statement_of_role(role: str, statements: Sequence[str] = DEFAULT_STATEMENTS) -> str | None:
    """ロール名 → 財務諸表の種類。``statements`` にない、または対象外なら None。

    複数の種類に当たるときは ``STATEMENT_ROLE_PATTERNS`` の順で最初のもの。
    """
    for statement, patterns in STATEMENT_ROLE_PATTERNS.items():
        if any(pattern in role for pattern in patterns):
            return statement if statement in statements else None
    return None


# ---------------------------------------------------------------- presentation の木


@dataclass
class RoleTree:
    """1ロールの presentation の木。"""

    order: dict[str, int] = field(default_factory=dict)  # key → 通し番号（深さ優先の順）
    order_path: dict[str, str] = field(default_factory=dict)  # key → "1_3_2" のような並び
    layer: dict[str, int] = field(default_factory=dict)  # key → 深さ（根が 0）
    parents: dict[str, list[str]] = field(default_factory=dict)  # key → 親科目（昇順）
    preferred_label: dict[str, str | None] = field(default_factory=dict)

    @property
    def keys(self) -> set[str]:
        return set(self.order)


def build_role_tree(arcs: pd.DataFrame) -> RoleTree:
    """presentation の arc から、表示順・階層・親科目を求める。

    根（親を持たない科目）は linkbase に現れた順、子は arc の order の順（同じ order は現れた順）に、
    深さ優先でたどる。同じ科目が木の中に複数回現れるときは、最初に現れた位置を使う。
    """
    tree = RoleTree()
    children: dict[str, list[tuple[float, int, str]]] = defaultdict(list)
    parents: dict[str, set[str]] = defaultdict(set)
    first_seen: dict[str, int] = {}
    for i, arc in enumerate(arcs.itertuples(index=False)):
        children[arc.parent_key].append((arc.order, i, arc.child_key))
        parents[arc.child_key].add(arc.parent_key)
        first_seen.setdefault(arc.parent_key, i)
        first_seen.setdefault(arc.child_key, i)
        if tree.preferred_label.get(arc.child_key) is None:
            tree.preferred_label[arc.child_key] = arc.preferred_label

    def visit(key: str, path: tuple[int, ...], stack: frozenset[str]) -> None:
        if key in tree.order or key in stack:  # 2回目以降・循環は無視
            return
        tree.order[key] = len(tree.order) + 1
        tree.order_path[key] = "_".join(map(str, path))
        tree.layer[key] = len(path) - 1
        for index, (_, _, child) in enumerate(sorted(children.get(key, [])), start=1):
            visit(child, (*path, index), stack | {key})

    roots = sorted((k for k in first_seen if k not in parents), key=first_seen.get)
    # 循環だけでできた部分（根がない）は、現れた順に最初の科目を根とみなす
    roots += [k for k in sorted(first_seen, key=first_seen.get) if k not in roots]
    index = 0
    for root in roots:
        if root not in tree.order:
            index += 1
            visit(root, (index,), frozenset())
    tree.parents = {key: sorted(values) for key, values in parents.items()}
    return tree


# ---------------------------------------------------------------- calculation


def calculation_effects(calc: pd.DataFrame, roots: Iterable[str]) -> dict[str, int]:
    """計算関係をたどり、各科目が root（例：Assets）を増やす（+1）か減らす（-1）か（届かなければ 0）。

    親が複数ある科目は、root に届く親のうち、親の科目名の昇順で最初のものを使う。
    """
    roots = set(roots)
    weights: dict[str, dict[str, float]] = defaultdict(dict)  # child → {parent: weight}
    for arc in calc.itertuples(index=False):
        weights[arc.child_key].setdefault(arc.parent_key, arc.weight)
    memo: dict[str, int] = {}

    def effect(key: str, visiting: frozenset[str]) -> int:
        if key in roots:
            return 1
        if key in memo:
            return memo[key]
        if key in visiting:
            return 0
        result = 0
        for parent in sorted(weights.get(key, {})):
            parent_effect = effect(parent, visiting | {key})
            if parent_effect:
                result = parent_effect * (1 if weights[key][parent] > 0 else -1)
                break
        memo[key] = result
        return result

    return {key: effect(key, frozenset()) for key in set(calc.parent_key) | set(calc.child_key)}


def debit_credit_keys(calc: pd.DataFrame) -> tuple[set[str], set[str]]:
    """借方・貸方の科目。

    debit：BS で資産を増やす科目、PL で当期純利益を減らす科目。credit：BS で負債純資産を増やす科目、
    PL で当期純利益を増やす科目。書類の BS・PL の全ロールの和集合をとる。
    """
    debit, credit = set(ASSETS), set(LIABILITIES_AND_NET_ASSETS) | set(PROFIT_LOSS)
    for role, arcs in calc.groupby("role"):
        statement = statement_of_role(role)
        if statement == "BS":
            debit |= {k for k, v in calculation_effects(arcs, ASSETS).items() if v > 0}
            credit |= {k for k, v in calculation_effects(arcs, LIABILITIES_AND_NET_ASSETS).items() if v > 0}
        elif statement == "PL":
            effects = calculation_effects(arcs, PROFIT_LOSS)
            debit |= {k for k, v in effects.items() if v < 0}
            credit |= {k for k, v in effects.items() if v > 0}
    return debit, credit


def calculation_columns(calc: pd.DataFrame, keys: pd.Series) -> dict[str, pd.Series]:
    """1ロールの計算関係から、科目ごとの列（calculated_flg・net_flg・net_src_flg・calc_parent_key・calc_weight）。"""
    parents = set(calc.parent_key)
    net_parents = {key for key in parents if key.endswith("Net")}
    net_sources = set(calc.loc[calc.parent_key.isin(net_parents), "child_key"])
    first = calc.sort_values(["child_key", "parent_key"]).drop_duplicates("child_key").set_index("child_key")
    return {
        "calculated_flg": keys.isin(parents).astype(int),
        "net_flg": keys.isin(net_parents).astype(int),
        "net_src_flg": keys.isin(net_sources).astype(int),
        "calc_parent_key": keys.map(first.parent_key),
        "calc_weight": keys.map(first.weight).astype("float64"),
    }


# ---------------------------------------------------------------- 1書類


def assemble_fs_tbl(
    docid: str,
    facts: pd.DataFrame,
    presentation: pd.DataFrame,
    calculation: pd.DataFrame,
    labels: pd.DataFrame,
    accounting_standards: str | None,
    statements: Sequence[str] = DEFAULT_STATEMENTS,
) -> pd.DataFrame:
    """Fact・presentation・calculation・ラベルから fs_tbl を作る。"""
    debit, credit = debit_credit_keys(calculation)
    frames = []
    for role, arcs in presentation.groupby("role", sort=True):
        statement = statement_of_role(role, statements)
        if statement is None:
            continue
        tree = build_role_tree(arcs)
        data = facts[facts.key.isin(tree.keys)]
        consolidated_role = "Consolidated" in role
        non_consolidated = data.context_ref.str.contains("NonConsolidated").astype(int)
        # 連結のロールには連結の値、個別のロールには個別の値だけを残す（報告書本体のロールはすべて残す）
        if statement != "report":
            keep = non_consolidated == (0 if consolidated_role else 1)
            data, non_consolidated = data[keep], non_consolidated[keep]
        if data.empty:
            continue
        frames.append(
            data.assign(
                docid=docid,
                statement=statement,
                role=role,
                consolidated_role=int(consolidated_role),
                order=data.key.map(tree.order),
                order_path=data.key.map(tree.order_path),
                layer=data.key.map(tree.layer),
                parent_key_list=data.key.map(lambda k, p=tree.parents: p.get(k, [])),
                preferred_label=data.key.map(tree.preferred_label),
                non_consolidated_flg=non_consolidated,
                current_flg=data.context_ref.str.contains("CurrentYear").astype(int),
                prior_flg=data.context_ref.str.contains("Prior1Year").astype(int),
                debit_flg=data.key.isin(debit).astype(int),
                credit_flg=data.key.isin(credit).astype(int),
                accounting_standards=accounting_standards,
                **calculation_columns(calculation[calculation.role == role], data.key),
            )
        )
    if not frames:
        return FsTblDf.validate(pd.DataFrame(columns=FS_COLUMNS))
    table = pd.concat(frames, ignore_index=True).join(labels, on="key")
    table = table[FS_COLUMNS].sort_values(["role", "order", "context_ref"], kind="stable", ignore_index=True)
    return FsTblDf.validate(table)


@dataclass
class ParsedDocument:
    """``parse_document`` の結果。"""

    docid: str
    fs_tbl: pd.DataFrame
    info: dict = field(default_factory=dict)


def parse_document(
    zip_path: str | Path,
    taxonomy: TaxonomyCache,
    *,
    docid: str | None = None,
    statements: Sequence[str] = DEFAULT_STATEMENTS,
    work_dir: str | Path | None = None,
) -> ParsedDocument:
    """書類1件の ZIP から fs_tbl を作る。ZIP の展開は一時ディレクトリで行い、終わったら消す。"""
    zip_path = Path(zip_path)
    docid = docid or zip_path.stem
    archive = XbrlArchive.open(zip_path)
    presentation = (
        parse_presentation(archive.get("presentation"))
        if archive.get("presentation")
        else pd.DataFrame(columns=PRESENTATION_COLUMNS)
    )
    calculation = (
        parse_calculation(archive.get("calculation"))
        if archive.get("calculation")
        else pd.DataFrame(columns=CALCULATION_COLUMNS)
    )
    company_labels = pd.concat(
        [parse_labels(archive.get(k)) for k in ("label", "label_en") if archive.get(k)]
        or [pd.DataFrame(columns=LABEL_COLUMNS)],
        ignore_index=True,
    )
    with tempfile.TemporaryDirectory(prefix=f"{docid}_", dir=work_dir) as tmp:
        instance = archive.extract(Path(tmp))
        loaded = load_facts(instance, taxonomy, Path(tmp) / "arelle.log")
    # Arelle が読んだ後なら、書類が参照する版の共通ラベルがキャッシュにある
    imports = schema_imports(archive.get("schema"))
    labels, missing_labels = resolve_labels(company_labels, imports, taxonomy)
    fs_tbl = assemble_fs_tbl(
        docid, loaded.facts, presentation, calculation, labels, loaded.accounting_standards, statements
    )
    info = {
        "taxonomy_versions": imports,
        "accounting_standards": loaded.accounting_standards,
        "n_facts": len(loaded.facts),
        "n_rows": len(fs_tbl),
        "missing_common_labels": missing_labels,
        "arelle_errors": loaded.arelle_errors[:20],
    }
    return ParsedDocument(docid, fs_tbl, info)


def get_fs_tbl(
    zip_path: str | Path,
    taxonomy: TaxonomyCache,
    *,
    docid: str | None = None,
    statements: Sequence[str] = DEFAULT_STATEMENTS,
) -> pd.DataFrame:
    """書類1件の ZIP から fs_tbl を作る（``parse_document`` の表だけを返す）。"""
    return parse_document(zip_path, taxonomy, docid=docid, statements=statements).fs_tbl


# ---------------------------------------------------------------- 複数書類（大福帳）


@dataclass
class FsTableResult:
    """``build_fs_table`` の結果。"""

    table: pd.DataFrame  # 大福帳（FsTblDf に書類の情報の列を加えたもの）
    log: pd.DataFrame  # 書類ごとの処理結果（ProcessingLogDf）
    paths: dict[str, Path] = field(default_factory=dict)  # 保存したファイル


def build_fs_table(
    docids: Iterable[str],
    zip_dir: str | Path,
    taxonomy: TaxonomyCache,
    *,
    statements: Sequence[str] = DEFAULT_STATEMENTS,
    documents: pd.DataFrame | None = None,
    document_columns: Sequence[str] = ("filerName", "sector_label_33"),
    out_path: str | Path | None = None,
) -> FsTableResult:
    """複数の書類の fs_tbl を縦につなげた大福帳を作る。

    - 書類の ZIP は ``{zip_dir}/{docid}.zip``
    - 1件の失敗で止めず、理由を ``log`` に記録して次へ進む
    - ``documents``（書類一覧。列 ``docID`` を持つ）を渡すと、``document_columns`` の列を各行に付ける
    - ``out_path``（``.parquet`` または ``.csv``）を渡すと、大福帳と処理結果（``{名前}_log``）を保存し、
      それぞれにリネージ（``{名前}_lin.json``）を付ける
    """
    zip_dir = Path(zip_dir)
    docids = list(dict.fromkeys(docids))
    tables, log_rows = [], []
    for docid in tqdm(docids, desc="fs_tbl"):
        zip_path = zip_dir / f"{docid}.zip"
        row = {"docid": docid, "zip_path": str(zip_path), "status": "success", "n_rows": 0, "message": None}
        try:
            parsed = parse_document(zip_path, taxonomy, docid=docid, statements=statements)
            tables.append(parsed.fs_tbl)
            row.update(
                status="success" if len(parsed.fs_tbl) else "empty",
                n_rows=len(parsed.fs_tbl),
                accounting_standards=parsed.info["accounting_standards"],
                taxonomy_versions=",".join(f"{k}/{v}" for k, v in sorted(parsed.info["taxonomy_versions"].items())),
                message="; ".join(f"共通ラベルなし:{m}" for m in parsed.info["missing_common_labels"]) or None,
            )
        except Exception as exc:  # noqa: BLE001  書類単位の失敗は記録して続ける
            row.update(status="failure", message=f"{type(exc).__name__}: {exc}")
        log_rows.append(row)
    table = pd.concat(tables, ignore_index=True) if tables else FsTblDf.validate(pd.DataFrame(columns=FS_COLUMNS))
    if documents is not None:
        info = documents.drop_duplicates("docID").set_index("docID")[list(document_columns)]
        table = table.join(info, on="docid")
    table = FsBigTableDf.validate(table)
    log = ProcessingLogDf.validate(pd.DataFrame(log_rows, columns=list(ProcessingLogDf.to_schema().columns)))
    result = FsTableResult(table, log)
    if out_path is not None:
        result.paths = _save_fs_table(result, Path(out_path), zip_dir, statements, document_columns if documents is not None else ())
    return result


def _save_fs_table(
    result: FsTableResult,
    out_path: Path,
    zip_dir: Path,
    statements: Sequence[str],
    document_columns: Sequence[str],
) -> dict[str, Path]:
    log_path = out_path.with_name(f"{out_path.stem}_log{out_path.suffix}")
    n_success = int((result.log.status != "failure").sum())
    save_table(
        result.table,
        out_path,
        purpose="EDINET の有価証券報告書 XBRL から、財務諸表・報告書本体の Fact をラベル・表示順とともに1つの表にまとめた大福帳（One Big Table）",
        source_code=build_fs_table,
        source_data_list=[
            f"{zip_dir}/{{docid}}.zip（EDINET 書類取得 API の XBRL の ZIP。{n_success} 書類。個別には記載しない。一覧は {log_path.name}）",
            "EDINET タクソノミ（書類が参照する版。TaxonomyCache 経由）",
        ],
        data_structure=(
            "1行＝1書類×1ロール×1科目×1 context の Fact。"
            f"対象のロール：{', '.join(statements)}。主キー：docid, role, key, context_ref。"
            + (f"書類一覧から付けた列：{', '.join(document_columns)}。" if document_columns else "")
        ),
        importance_columns={
            "docid": "書類管理番号（EDINET の docID）",
            "statement": "財務諸表の種類（BS・PL・CF・SS・CI・report・notes）",
            "key": "科目（接頭辞:要素名。例 jppfs_cor:NetSales）",
            "context_ref": "コンテキスト（連結／個別・当期／前期・次元を区別）",
            "data_str": "値（文字列。数値は preproc_num で数値に直す）",
            "label_jp": "日本語ラベル",
            "order": "ロール内の表示順（深さ優先の通し番号）",
            "non_consolidated_flg / current_flg / prior_flg": "個別・当期・前期の印",
        },
        schema=FsBigTableDf,
        run_config={"statements": list(statements)},
    )
    save_table(
        result.log,
        log_path,
        purpose=f"{out_path.name} を作ったときの書類ごとの処理結果（成功・対象なし・失敗と理由）",
        source_code=build_fs_table,
        source_data_list=[f"{zip_dir}/{{docid}}.zip（{len(result.log)} 書類。個別には記載しない）"],
        data_structure="1行＝1書類。status は success（行あり）・empty（対象のロールに Fact なし）・failure（理由は message）",
        importance_columns={"docid": "書類管理番号", "status": "処理結果", "message": "失敗の理由・注意"},
        schema=ProcessingLogDf,
    )
    return {"table": out_path, "log": log_path}

