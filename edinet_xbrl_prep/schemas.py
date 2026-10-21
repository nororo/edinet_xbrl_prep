"""Structures (pandera) of the tables this package produces."""

from __future__ import annotations

from typing import Optional  # noqa: UP035  pandera cannot read X | None annotations

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

String = pd.StringDtype
Int = pd.Int64Dtype
DATE = r"^\d{4}-\d{2}-\d{2}$"
FLAG = {"isin": [0, 1]}
DOCID = r"^S1[0-9A-Z]{6}$"


class DocumentListDf(pa.DataFrameModel):
    """Result of the document list API (v2, type=2). One row = one document (one entry of a day's list).

    Column names follow the API fields (see the EDINET API specification, version 2).
    ``access_date`` is the date of retrieval.
    """

    access_date: Series[String] = pa.Field(str_matches=DATE)  # date of retrieval
    date: Series[String] = pa.Field(str_matches=DATE)  # date passed to the document list API (submission date)
    seqNumber: Series[Int] = pa.Field(ge=1)  # sequence number in order of submission on the same day
    docID: Series[String] = pa.Field(str_matches=DOCID)  # document ID (file name of the ZIP)
    edinetCode: Series[String] = pa.Field(nullable=True)  # filer EDINET code
    secCode: Series[String] = pa.Field(nullable=True)  # securities code (5 digits)
    JCN: Series[String] = pa.Field(nullable=True)  # corporate number
    filerName: Series[String] = pa.Field(nullable=True)  # filer name
    fundCode: Series[String] = pa.Field(nullable=True)  # fund code
    ordinanceCode: Series[String] = pa.Field(nullable=True)  # ordinance code (010: Cabinet Office Ordinance on Disclosure of Corporate Affairs)
    formCode: Series[String] = pa.Field(nullable=True)  # form code (030000: annual securities report)
    docTypeCode: Series[String] = pa.Field(nullable=True)  # document type code (120: annual securities report, 130: amendment)
    periodStart: Series[String] = pa.Field(nullable=True)  # period start
    periodEnd: Series[String] = pa.Field(nullable=True)  # period end
    submitDateTime: Series[String] = pa.Field(nullable=True)  # submission date and time (YYYY-MM-DD HH:MM)
    docDescription: Series[String] = pa.Field(nullable=True)  # document description
    issuerEdinetCode: Series[String] = pa.Field(nullable=True)  # issuer EDINET code (large shareholding reports)
    subjectEdinetCode: Series[String] = pa.Field(nullable=True)  # subject EDINET code (tender offers)
    subsidiaryEdinetCode: Series[String] = pa.Field(nullable=True)  # subsidiary EDINET codes (comma-separated)
    currentReportReason: Series[String] = pa.Field(nullable=True)  # reasons for an extraordinary report
    parentDocID: Series[String] = pa.Field(nullable=True)  # parent document ID (e.g. the document an amendment corrects)
    opeDateTime: Series[String] = pa.Field(nullable=True)  # date and time of an operation by Local Finance Bureau staff
    withdrawalStatus: Series[String] = pa.Field(isin=["0", "1", "2"])  # 1: withdrawal notice, 2: withdrawn document
    docInfoEditStatus: Series[String] = pa.Field(isin=["0", "1", "2"])  # 1: correction information, 2: corrected document
    disclosureStatus: Series[String] = pa.Field(isin=["0", "1", "2", "3"])  # non-disclosure status
    xbrlFlag: Series[String] = pa.Field(isin=["0", "1"])
    pdfFlag: Series[String] = pa.Field(isin=["0", "1"])
    attachDocFlag: Series[String] = pa.Field(isin=["0", "1"])
    englishDocFlag: Series[String] = pa.Field(isin=["0", "1"])
    csvFlag: Series[String] = pa.Field(isin=["0", "1"])
    legalStatus: Series[String] = pa.Field(isin=["0", "1", "2"])  # 1: on public inspection, 2: extended period, 0: inspection period ended
    sector_code_33: Optional[Series[String]] = pa.Field(nullable=True)
    sector_label_33: Optional[Series[String]] = pa.Field(nullable=True)  # TSE 33-industry classification
    sector_code_17: Optional[Series[String]] = pa.Field(nullable=True)
    sector_label_17: Optional[Series[String]] = pa.Field(nullable=True)

    @pa.dataframe_check
    def unique_document(cls, df: pd.DataFrame) -> bool:
        """Unique by (requested date, document ID)."""
        return not df.duplicated(["date", "docID"]).any()

    class Config:
        strict = True
        coerce = True


class FsTblDf(pa.DataFrameModel):
    """Per-document table (fs_tbl). One row = one fact of document x role x account x context."""

    docid: Series[String] = pa.Field(str_matches=DOCID)  # document ID
    statement: Series[String] = pa.Field(isin=["BS", "PL", "CF", "SS", "CI", "report", "notes"])
    role: Series[String]  # presentation role (e.g. rol_ConsolidatedBalanceSheet)
    consolidated_role: Series[Int] = pa.Field(**FLAG)  # whether the role is consolidated
    key: Series[String] = pa.Field(str_contains=":")  # account (prefix:ElementName)
    element_name: Series[String]
    context_ref: Series[String]  # context ID (e.g. CurrentYearInstant_NonConsolidatedMember)
    data_str: Series[String] = pa.Field(nullable=True)  # value (missing for xsi:nil)
    is_nil: Series[Int] = pa.Field(**FLAG)
    balance: Series[String] = pa.Field(nullable=True, isin=["debit", "credit"])  # balance attribute of the concept definition
    decimals: Series[String] = pa.Field(nullable=True)  # -3: thousands of yen, -6: millions of yen, INF, etc.
    unit: Series[String] = pa.Field(nullable=True)  # JPY, pure, shares, etc.
    period_type: Series[String] = pa.Field(nullable=True, isin=["instant", "duration"])
    is_text_block: Series[Int] = pa.Field(**FLAG)
    is_abstract: Series[Int] = pa.Field(**FLAG)
    period_start: Series[String] = pa.Field(nullable=True, str_matches=DATE)  # period start of a duration
    period_end: Series[String] = pa.Field(nullable=True, str_matches=DATE)  # period end of a duration; the instant date for an instant
    instant_date: Series[String] = pa.Field(nullable=True, str_matches=DATE)  # date of an instant
    dimensions: Series[String] = pa.Field(nullable=True)  # dimensions and members (JSON; segments, etc.)
    n_source_facts: Series[Int] = pa.Field(ge=1)  # number of merged source facts (usually 1)
    label_jp: Series[String] = pa.Field(nullable=True)
    label_jp_long: Series[String] = pa.Field(nullable=True)
    label_en: Series[String] = pa.Field(nullable=True)
    label_en_long: Series[String] = pa.Field(nullable=True)
    label_source: Series[String] = pa.Field(nullable=True, isin=["company", "common"])
    preferred_label: Series[String] = pa.Field(nullable=True)  # label role used for display (totalLabel, etc.)
    order: Series[Int] = pa.Field(ge=1)  # display order in the role (depth-first sequence number)
    order_path: Series[String] = pa.Field(str_matches=r"^\d+(_\d+)*$")  # position path from the root (e.g. 1_3_2)
    layer: Series[Int] = pa.Field(ge=0)  # depth in the hierarchy (root is 0)
    parent_key_list: Series[object]  # parent accounts in the presentation (list)
    non_consolidated_flg: Series[Int] = pa.Field(**FLAG)  # the context is non-consolidated
    current_flg: Series[Int] = pa.Field(**FLAG)  # the context is the current year
    prior_flg: Series[Int] = pa.Field(**FLAG)  # the context is the prior year
    calculated_flg: Series[Int] = pa.Field(**FLAG)  # parent account in the calculation linkbase
    net_flg: Series[Int] = pa.Field(**FLAG)  # calculation parent account whose name ends with "Net"
    net_src_flg: Series[Int] = pa.Field(**FLAG)  # child account of such a parent
    calc_parent_key: Series[String] = pa.Field(nullable=True)
    calc_weight: Series[float] = pa.Field(nullable=True)
    debit_flg: Series[Int] = pa.Field(**FLAG)  # debit (increases assets or decreases profit)
    credit_flg: Series[Int] = pa.Field(**FLAG)  # credit (increases liabilities and net assets, or increases profit)
    accounting_standards: Series[String] = pa.Field(nullable=True)  # accounting standards (Japan GAAP, IFRS, US GAAP)

    @pa.dataframe_check
    def unique_row(cls, df: pd.DataFrame) -> bool:
        """Unique by (document, role, account, context)."""
        return not df.duplicated(["docid", "role", "key", "context_ref"]).any()

    @pa.dataframe_check
    def net_implies_calculated(cls, df: pd.DataFrame) -> Series[bool]:
        """An account with net_flg = 1 is a calculation parent."""
        return (df.net_flg == 0) | (df.calculated_flg == 1)

    class Config:
        strict = True
        coerce = True


class FsBigTableDf(FsTblDf):
    """One Big Table: fs_tbl of many documents plus columns from the document list (filer name, industry, etc.)."""

    filerName: Optional[Series[String]] = pa.Field(nullable=True)  # filer name
    sector_label_33: Optional[Series[String]] = pa.Field(nullable=True)  # TSE 33-industry classification

    class Config:
        strict = False  # other columns of the document list may be attached
        coerce = True


class NumericFsTblDf(pa.DataFrameModel):
    """Columns added by ``preproc_num``."""

    data: Series[float] = pa.Field(nullable=True)  # data_str converted to a number (missing if not convertible)
    context_ref_len: Series[int] = pa.Field(ge=1)  # number of "_"-separated parts of context_ref (fewer = more basic value)

    class Config:
        strict = False
        coerce = True


class ProcessingLogDf(pa.DataFrameModel):
    """Per-document processing result of ``build_fs_table``."""

    docid: Series[String]
    zip_path: Series[String]
    status: Series[String] = pa.Field(isin=["success", "empty", "failure"])
    n_rows: Series[Int] = pa.Field(ge=0)
    accounting_standards: Series[String] = pa.Field(nullable=True)
    taxonomy_versions: Series[String] = pa.Field(nullable=True)
    message: Series[String] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True


class DownloadLogDf(pa.DataFrameModel):
    """Per-document result of ``download_documents``."""

    docid: Series[String]
    status: Series[String] = pa.Field(isin=["success", "skipped", "failure"])
    path: Series[String] = pa.Field(nullable=True)
    bytes: Series[Int] = pa.Field(nullable=True, ge=0)
    message: Series[String] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
