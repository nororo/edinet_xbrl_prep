"""EDINET の有価証券報告書 XBRL から、財務数値・テキストの大福帳（One Big Table）を作る。

主な流れ（README 参照）:
1. ``fetch_document_lists`` → ``documents_frame`` → ``select_reports``：書類一覧を取得し、有価証券報告書を選ぶ
2. ``download_documents``：書類の ZIP をダウンロードする
3. ``TaxonomyCache``：タクソノミのキャッシュ（Arelle と共通ラベルが読む）
4. ``build_fs_table``：大福帳を作り、リネージ付きで保存する
"""

from edinet_xbrl_prep.edinet_api import (
    add_tse_sector,
    documents_frame,
    download_document,
    download_documents,
    failed_days,
    fetch_document_list,
    fetch_document_lists,
    load_day_results,
    read_tse_sector,
    save_day_results,
    save_document_list,
    select_reports,
)
from edinet_xbrl_prep.fs_tbl import (
    STATEMENT_ROLE_PATTERNS,
    build_fs_table,
    get_fs_tbl,
    parse_document,
)
from edinet_xbrl_prep.lineage import read_table, save_table
from edinet_xbrl_prep.llm import GroqAPI, Prompt
from edinet_xbrl_prep.numeric import fill_df, pick_base_context, preproc_num
from edinet_xbrl_prep.schemas import DocumentListDf, FsBigTableDf, FsTblDf
from edinet_xbrl_prep.taxonomy import TaxonomyCache, download_fsa_taxonomy, seed_from_taxonomy_zip
from edinet_xbrl_prep.text import preproc_nlp

__all__ = [
    "DocumentListDf",
    "FsBigTableDf",
    "FsTblDf",
    "GroqAPI",
    "Prompt",
    "STATEMENT_ROLE_PATTERNS",
    "TaxonomyCache",
    "add_tse_sector",
    "build_fs_table",
    "documents_frame",
    "download_document",
    "download_documents",
    "download_fsa_taxonomy",
    "failed_days",
    "fetch_document_list",
    "fetch_document_lists",
    "fill_df",
    "get_fs_tbl",
    "load_day_results",
    "parse_document",
    "pick_base_context",
    "preproc_nlp",
    "preproc_num",
    "read_table",
    "read_tse_sector",
    "save_day_results",
    "save_document_list",
    "save_table",
    "seed_from_taxonomy_zip",
    "select_reports",
]
