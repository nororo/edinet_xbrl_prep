"""Arelle で XBRL インスタンスの Fact を取り出す。

旧版（``xbrl_parser_rapper.get_xbrl_rapper``・``get_xbrl_df``・``get_xbrl_dei_df``）との違い:
- 期末日・時点：Arelle は日付だけの ``endDate``・``instant`` を「翌日0時」として持つため、旧版の
  ``period_end``・``instant_date`` は1日後ろにずれていた。ここでは XBRL に書かれた日付（propertyView）を使う。
  旧版の ``end_date_pv``・``instant_date_pv`` は、直した ``period_end``・``instant_date`` と同じになるので廃止
- 会計基準（``jpdei_cor:AccountingStandardsDEI``）は同じ読み込みで取り出す（旧版は書類を2回読み込んでいた）
- 例外を握りつぶさない（旧版は例外ハンドラ内で再び例外が出ることがあった）。失敗は呼び出し側で記録する
- 同じ要素・同じ context の Fact が複数あるとき（提出データの重複）は1行にまとめ、元の数を残す
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from edinet_xbrl_prep.taxonomy import TaxonomyCache

ACCOUNTING_STANDARDS_KEY = "jpdei_cor:AccountingStandardsDEI"
FACT_COLUMNS = [
    "key",
    "data_str",
    "decimals",
    "context_ref",
    "element_name",
    "unit",
    "period_type",
    "is_text_block",
    "is_abstract",
    "is_nil",
    "balance",
    "period_start",
    "period_end",
    "instant_date",
    "dimensions",
]


class XbrlLoadError(Exception):
    """Arelle が書類を読めなかった（Fact が0件）。"""


@dataclass
class FactsResult:
    facts: pd.DataFrame
    accounting_standards: str | None
    arelle_errors: list[str] = field(default_factory=list)


def _date_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else None


def context_dates(context) -> tuple[str | None, str | None, str | None]:
    """（期首日, 期末日, 時点）。XBRL に書かれた日付（propertyView）を使う。

    時点（instant）の Fact は、期末日にも時点と同じ日を入れる（旧版と同じ扱い）。
    """
    view = dict(item for item in (context.propertyView or ()) if item and len(item) == 2)
    if context.isInstantPeriod:
        instant = view.get("instant")
        if instant is None and context.instantDatetime is not None:
            instant = context.instantDatetime - dt.timedelta(days=1)
        return None, _date_text(instant), _date_text(instant)
    if context.isStartEndPeriod:
        start = view.get("startDate") or context.startDatetime
        end = view.get("endDate")
        if end is None and context.endDatetime is not None:
            end = context.endDatetime - dt.timedelta(days=1)
        return _date_text(start), _date_text(end), None
    return None, None, None


def context_dimensions(context) -> str | None:
    """シナリオの次元とメンバー（例：連結・個別、セグメント）を JSON 文字列で。"""
    dims = context.qnameDims
    if not dims:
        return None
    pairs = {
        str(dim): str(value.memberQname) if value.isExplicit else str(value.typedMember)
        for dim, value in sorted(dims.items(), key=lambda item: str(item[0]))
    }
    return json.dumps(pairs, ensure_ascii=False, sort_keys=True)


def load_facts(instance: Path, taxonomy: TaxonomyCache, log_file: Path) -> FactsResult:
    """インスタンスを読み、Fact の表と会計基準を返す。"""
    controller = taxonomy.controller(log_file)
    try:
        model = controller.modelManager.load(str(instance))
        errors = [str(e) for e in (model.errors or [])]
        rows = []
        accounting_standards = None
        for fact in model.facts:
            concept, context = fact.concept, fact.context
            if concept is None or context is None:
                continue
            key = f"{fact.qname.prefix}:{fact.qname.localName}"
            if key == ACCOUNTING_STANDARDS_KEY:
                accounting_standards = fact.value
            start, end, instant = context_dates(context)
            rows.append(
                {
                    "key": key,
                    "data_str": None if fact.isNil else fact.value,
                    "decimals": fact.decimals,
                    "context_ref": fact.contextID,
                    "element_name": fact.qname.localName,
                    "unit": fact.unitID,
                    "period_type": concept.periodType,
                    "is_text_block": int(bool(concept.isTextBlock)),
                    "is_abstract": int(concept.abstract == "true"),
                    "is_nil": int(bool(fact.isNil)),
                    "balance": concept.balance,
                    "period_start": start,
                    "period_end": end,
                    "instant_date": instant,
                    "dimensions": context_dimensions(context),
                }
            )
    finally:
        controller.close()
    if not rows:
        raise XbrlLoadError(f"Fact が0件: {Path(instance).name}（Arelle のエラー: {errors[:3]}）")
    facts = pd.DataFrame(rows, columns=FACT_COLUMNS).drop_duplicates(ignore_index=True)
    return FactsResult(merge_duplicate_facts(facts), accounting_standards, errors)


def merge_duplicate_facts(facts: pd.DataFrame) -> pd.DataFrame:
    """同じ（要素, context）で値の違う Fact を1行にまとめ、元の Fact の数を ``n_source_facts`` に残す。

    - テキストブロック：本文を現れた順に改行でつなぐ（どちらの本文も捨てない）
    - それ以外（値の食い違う数値など）：最初の Fact を残す
    """
    facts = facts.assign(n_source_facts=1)
    keys = ["key", "context_ref"]
    duplicated = facts.duplicated(keys, keep=False)
    if not duplicated.any():
        return facts
    merged = []
    for _, group in facts[duplicated].groupby(keys, sort=False):
        first = group.iloc[0].copy()
        if first.is_text_block == 1:
            texts = [t for t in group.data_str if t is not None]
            first["data_str"] = "\n".join(texts) if texts else None
        first["n_source_facts"] = len(group)
        merged.append(first)
    out = pd.concat([facts[~duplicated], pd.DataFrame(merged)])
    return out.sort_index(kind="stable").reset_index(drop=True)
