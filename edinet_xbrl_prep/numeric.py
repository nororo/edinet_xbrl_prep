"""大福帳の数値の前処理（旧 ``num_prep.py``）。

旧版との違い:
- ``fill_df`` の補完を、書類の中・科目の中に限った（旧版は並べ替えた表全体で前の行の値を引き継いだため、
  別の書類・別の科目の値が入ることがあった）。pandas 2.2 以降で廃止予定の ``fillna(method=...)`` をやめた
- ``fill_df`` は、提出者名・業種の列がなくても動く（あれば書類の中で補う）
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from edinet_xbrl_prep.schemas import NumericFsTblDf

# 書類（docid）ごとに決まる列と、科目（key）ごとに決まる列。fill_df で補う
DOCUMENT_LEVEL_COLUMNS = (
    "decimals", "context_ref", "unit", "period_start", "period_end", "instant_date", "order",
    "non_consolidated_flg", "current_flg", "prior_flg", "role", "statement", "filerName", "sector_label_33",
)
KEY_LEVEL_COLUMNS = ("element_name", "is_text_block", "is_abstract", "label_jp", "label_en")


def preproc_num(fs_tbl: pd.DataFrame) -> pd.DataFrame:
    """``data_str`` を数値の列 ``data`` に直し、``context_ref_len``（context を ``_`` で分けた要素の数）を加える。

    同じ科目に複数の context（セグメントなど）がある場合、``context_ref_len`` が最も小さい行が基本の値。
    """
    data = fs_tbl.assign(
        data=pd.to_numeric(fs_tbl.data_str, errors="coerce").astype(float),
        context_ref_len=fs_tbl.context_ref.str.count("_") + 1,
    )
    return NumericFsTblDf.validate(data)


def pick_base_context(data: pd.DataFrame) -> pd.DataFrame:
    """書類・科目ごとに ``context_ref_len`` が最も小さい1行を残す（旧版ノートブックの並べ替えと重複削除）。"""
    return data.sort_values("context_ref_len", kind="stable").drop_duplicates(["docid", "key"], keep="first")


def fill_df(data: pd.DataFrame) -> pd.DataFrame:
    """書類×科目の全組み合わせの表にし、ない組み合わせを補う（構成比の分析用）。

    - 値（``data``）：組み合わせがない（科目が書類にない）→ 0、値が 0 → 表示単位の半分（0.5 × 10^(-decimals)）。
      旧版と同じ規則（対数をとる分析で 0 を避けるため）
    - 書類ごとの列（decimals など）は同じ書類の他の行から、科目ごとの列（ラベルなど）は同じ科目の他の行から補う
    """
    docs = data[["docid"]].drop_duplicates()
    keys = data[["key"]].drop_duplicates()
    merged = docs.merge(keys, how="cross").merge(data, on=["docid", "key"], how="left")
    missing = merged.data.isna() & ~merged.set_index(["docid", "key"]).index.isin(
        data.set_index(["docid", "key"]).index
    )
    for columns, by in ((DOCUMENT_LEVEL_COLUMNS, "docid"), (KEY_LEVEL_COLUMNS, "key")):
        present = [c for c in columns if c in merged]
        merged[present] = merged.groupby(by)[present].transform(lambda s: s.ffill().bfill())
    half_unit = 0.5 * np.power(10.0, -merged.decimals.astype(float))
    value = merged.data.mask(merged.data == 0, half_unit)
    merged["data"] = value.mask(missing, 0.0)
    return merged
