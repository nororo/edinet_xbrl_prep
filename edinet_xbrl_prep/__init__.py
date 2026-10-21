"""Build a One Big Table of financial values and text from EDINET annual securities report XBRL.

Main flow (see README):
1. ``fetch_document_lists`` -> ``documents_frame`` -> ``select_reports``: fetch the document list and select annual reports
2. ``download_documents``: download the document ZIP files
3. ``TaxonomyCache``: taxonomy cache (read by Arelle and for the common labels)
4. ``build_fs_table``: build the One Big Table and save it with lineage
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
