"""Preprocessing of numeric values in the One Big Table."""

from __future__ import annotations

import numpy as np
import pandas as pd

from edinet_xbrl_prep.schemas import NumericFsTblDf

# Columns determined per document (docid) and per account (key); filled by fill_df
DOCUMENT_LEVEL_COLUMNS = (
    "decimals", "context_ref", "unit", "period_start", "period_end", "instant_date", "order",
    "non_consolidated_flg", "current_flg", "prior_flg", "role", "statement", "filerName", "sector_label_33",
)
KEY_LEVEL_COLUMNS = ("element_name", "is_text_block", "is_abstract", "label_jp", "label_en")


def preproc_num(fs_tbl: pd.DataFrame) -> pd.DataFrame:
    """Convert ``data_str`` to the numeric column ``data`` and add ``context_ref_len``
    (the number of ``_``-separated parts of the context).

    When an account has several contexts (segments, etc.), the row with the smallest ``context_ref_len``
    holds the base value.
    """
    data = fs_tbl.assign(
        data=pd.to_numeric(fs_tbl.data_str, errors="coerce").astype(float),
        context_ref_len=fs_tbl.context_ref.str.count("_") + 1,
    )
    return NumericFsTblDf.validate(data)


def pick_base_context(data: pd.DataFrame) -> pd.DataFrame:
    """Keep the row with the smallest ``context_ref_len`` for each document and account."""
    return data.sort_values("context_ref_len", kind="stable").drop_duplicates(["docid", "key"], keep="first")


def fill_df(data: pd.DataFrame) -> pd.DataFrame:
    """Expand to every document x account combination and fill the missing ones (for composition analysis).

    - Value (``data``): a missing combination (the account is not in the document) -> 0;
      a value of 0 -> half the display unit (0.5 x 10^(-decimals))
    - Per-document columns (decimals, etc.) are filled from other rows of the same document,
      per-account columns (labels, etc.) from other rows of the same account
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
