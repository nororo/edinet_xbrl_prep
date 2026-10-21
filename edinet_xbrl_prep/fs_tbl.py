"""Build the per-document table (fs_tbl) and the One Big Table of many documents.

One row of fs_tbl = one fact of document x role x account x context, with the presentation order, hierarchy and
parent accounts, calculation relationships, labels, debit/credit flags, and consolidated/non-consolidated and
current/prior year flags.
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

# statement kind -> strings "contained" in the role name (e.g. rol_ConsolidatedBalanceSheet)
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

# calculation roots used to determine debit/credit
ASSETS = ("jppfs_cor:Assets", "jpigp_cor:AssetsIFRS")
LIABILITIES_AND_NET_ASSETS = ("jppfs_cor:LiabilitiesAndNetAssets", "jpigp_cor:LiabilitiesAndEquityIFRS")
PROFIT_LOSS = ("jppfs_cor:ProfitLoss", "jpigp_cor:ProfitLossIFRS")

FS_COLUMNS = list(FsTblDf.to_schema().columns)


def statement_of_role(role: str, statements: Sequence[str] = DEFAULT_STATEMENTS) -> str | None:
    """Role name -> statement kind. None if the kind is not in ``statements`` or the role is not covered.

    If several kinds match, the first in ``STATEMENT_ROLE_PATTERNS`` order is used.
    """
    for statement, patterns in STATEMENT_ROLE_PATTERNS.items():
        if any(pattern in role for pattern in patterns):
            return statement if statement in statements else None
    return None


# ---------------------------------------------------------------- presentation tree


@dataclass
class RoleTree:
    """Presentation tree of one role."""

    order: dict[str, int] = field(default_factory=dict)  # key -> sequence number (depth-first order)
    order_path: dict[str, str] = field(default_factory=dict)  # key -> position path such as "1_3_2"
    layer: dict[str, int] = field(default_factory=dict)  # key -> depth (root is 0)
    parents: dict[str, list[str]] = field(default_factory=dict)  # key -> parent accounts (sorted)
    preferred_label: dict[str, str | None] = field(default_factory=dict)

    @property
    def keys(self) -> set[str]:
        return set(self.order)


def build_role_tree(arcs: pd.DataFrame) -> RoleTree:
    """Compute the display order, hierarchy and parent accounts from presentation arcs.

    Traverse depth-first: roots (accounts without a parent) in order of appearance in the linkbase, children
    in arc ``order`` (ties in order of appearance). An account appearing more than once uses its first position.
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
        if key in tree.order or key in stack:  # skip repeated visits and cycles
            return
        tree.order[key] = len(tree.order) + 1
        tree.order_path[key] = "_".join(map(str, path))
        tree.layer[key] = len(path) - 1
        for index, (_, _, child) in enumerate(sorted(children.get(key, [])), start=1):
            visit(child, (*path, index), stack | {key})

    roots = sorted((k for k in first_seen if k not in parents), key=first_seen.get)
    # for parts made only of cycles (no root), treat the first account in order of appearance as a root
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
    """Follow the calculation relationships: whether each account increases (+1) or decreases (-1) a root
    (e.g. Assets), or 0 if it does not reach one.

    For an account with several parents, the first parent (by name) that reaches a root is used.
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
    """Debit and credit accounts.

    debit: accounts that increase assets in the BS or decrease profit in the PL. credit: accounts that increase
    liabilities and net assets in the BS or increase profit in the PL. The union over all BS and PL roles
    of the document is taken.
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
    """Per-account columns from the calculation relationships of one role (calculated_flg, net_flg, net_src_flg, calc_parent_key, calc_weight)."""
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


# ---------------------------------------------------------------- one document


def assemble_fs_tbl(
    docid: str,
    facts: pd.DataFrame,
    presentation: pd.DataFrame,
    calculation: pd.DataFrame,
    labels: pd.DataFrame,
    accounting_standards: str | None,
    statements: Sequence[str] = DEFAULT_STATEMENTS,
) -> pd.DataFrame:
    """Build fs_tbl from facts, presentation, calculation and labels."""
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
        # keep only consolidated values in consolidated roles and non-consolidated values in other roles (report roles keep all)
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
    """Result of ``parse_document``."""

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
    """Build fs_tbl from the ZIP of one document. Files are extracted to a temporary directory that is removed afterwards."""
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
    # after Arelle has loaded the document, the common labels of the referenced versions are in the cache
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
    """Build fs_tbl from the ZIP of one document (returns only the table of ``parse_document``)."""
    return parse_document(zip_path, taxonomy, docid=docid, statements=statements).fs_tbl


# ---------------------------------------------------------------- many documents (One Big Table)


@dataclass
class FsTableResult:
    """Result of ``build_fs_table``."""

    table: pd.DataFrame  # One Big Table (FsTblDf plus document information columns)
    log: pd.DataFrame  # per-document processing results (ProcessingLogDf)
    paths: dict[str, Path] = field(default_factory=dict)  # saved files


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
    """Build the One Big Table by stacking the fs_tbl of many documents.

    - Document ZIPs are at ``{zip_dir}/{docid}.zip``
    - A failing document does not stop the run; the reason is recorded in ``log``
    - With ``documents`` (document list with a ``docID`` column), the ``document_columns`` are added to each row
    - With ``out_path`` (``.parquet`` or ``.csv``), the table and the processing results (``{name}_log``) are saved,
      each with its lineage (``{name}_lin.json``)
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
        except Exception as exc:  # noqa: BLE001  record per-document failures and continue
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
