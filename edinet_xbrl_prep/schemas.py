"""このパッケージが作る表の構造（pandera）。

旧版では ``EdinetResponseDf``（edinet_api.py）、``FsDataDf``（fs_tbl.py）、``xbrl_elm_schima``・
``ParentChildLink`` など（xbrl_parser_rapper.py・link_base_file_analyzer.py）に分かれていた。
外に出す表のスキーマをここにまとめ、linkbase の中間表のスキーマは廃止した（関数の戻り値の列で表す）。
"""

from __future__ import annotations

from typing import Optional  # noqa: UP035  pandera は X | None の注釈を読めない

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

String = pd.StringDtype
Int = pd.Int64Dtype
DATE = r"^\d{4}-\d{2}-\d{2}$"
FLAG = {"isin": [0, 1]}
DOCID = r"^S1[0-9A-Z]{6}$"


class DocumentListDf(pa.DataFrameModel):
    """書類一覧 API（v2, type=2）の結果。1行＝1書類（1日の一覧の1件）。

    列名は API の項目名のまま（参考：EDINET API 仕様書 version 2）。``access_date`` は取得した日。
    ``sector_*`` は ``add_tse_sector`` で付ける東証の業種区分（取得した時点の区分）。
    """

    access_date: Series[String] = pa.Field(str_matches=DATE)  # 取得した日
    date: Series[String] = pa.Field(str_matches=DATE)  # 書類一覧 API に指定した日（提出日）
    seqNumber: Series[Int] = pa.Field(ge=1)  # 同じ日の提出順の連番
    docID: Series[String] = pa.Field(str_matches=DOCID)  # 書類管理番号（ZIP のファイル名）
    edinetCode: Series[String] = pa.Field(nullable=True)  # 提出者 EDINET コード
    secCode: Series[String] = pa.Field(nullable=True)  # 証券コード（5桁）
    JCN: Series[String] = pa.Field(nullable=True)  # 法人番号
    filerName: Series[String] = pa.Field(nullable=True)  # 提出者名
    fundCode: Series[String] = pa.Field(nullable=True)  # ファンドコード
    ordinanceCode: Series[String] = pa.Field(nullable=True)  # 府令コード（010：企業内容等の開示に関する内閣府令）
    formCode: Series[String] = pa.Field(nullable=True)  # 様式コード（030000：有価証券報告書）
    docTypeCode: Series[String] = pa.Field(nullable=True)  # 書類種別コード（120：有価証券報告書、130：訂正）
    periodStart: Series[String] = pa.Field(nullable=True)  # 期間（自）
    periodEnd: Series[String] = pa.Field(nullable=True)  # 期間（至）
    submitDateTime: Series[String] = pa.Field(nullable=True)  # 提出日時（YYYY-MM-DD HH:MM）
    docDescription: Series[String] = pa.Field(nullable=True)  # 提出書類概要
    issuerEdinetCode: Series[String] = pa.Field(nullable=True)  # 発行会社 EDINET コード（大量保有）
    subjectEdinetCode: Series[String] = pa.Field(nullable=True)  # 対象 EDINET コード（公開買付け）
    subsidiaryEdinetCode: Series[String] = pa.Field(nullable=True)  # 子会社 EDINET コード（カンマ区切り）
    currentReportReason: Series[String] = pa.Field(nullable=True)  # 臨時報告書の提出事由
    parentDocID: Series[String] = pa.Field(nullable=True)  # 親書類管理番号（訂正報告書の訂正対象など）
    opeDateTime: Series[String] = pa.Field(nullable=True)  # 財務局職員による操作の日時
    withdrawalStatus: Series[String] = pa.Field(isin=["0", "1", "2"])  # 1：取下書、2：取り下げられた書類
    docInfoEditStatus: Series[String] = pa.Field(isin=["0", "1", "2"])  # 1：修正情報、2：修正された書類
    disclosureStatus: Series[String] = pa.Field(isin=["0", "1", "2", "3"])  # 不開示の状態
    xbrlFlag: Series[String] = pa.Field(isin=["0", "1"])
    pdfFlag: Series[String] = pa.Field(isin=["0", "1"])
    attachDocFlag: Series[String] = pa.Field(isin=["0", "1"])
    englishDocFlag: Series[String] = pa.Field(isin=["0", "1"])
    csvFlag: Series[String] = pa.Field(isin=["0", "1"])
    legalStatus: Series[String] = pa.Field(isin=["0", "1", "2"])  # 1：縦覧中、2：延長期間中、0：閲覧期間満了
    sector_code_33: Optional[Series[String]] = pa.Field(nullable=True)
    sector_label_33: Optional[Series[String]] = pa.Field(nullable=True)  # 東証33業種区分
    sector_code_17: Optional[Series[String]] = pa.Field(nullable=True)
    sector_label_17: Optional[Series[String]] = pa.Field(nullable=True)

    @pa.dataframe_check
    def unique_document(cls, df: pd.DataFrame) -> bool:
        """（指定した日, 書類管理番号）で一意。"""
        return not df.duplicated(["date", "docID"]).any()

    class Config:
        strict = True
        coerce = True


class FsTblDf(pa.DataFrameModel):
    """書類ごとの表（fs_tbl）。1行＝1書類×1ロール×1科目×1 context の Fact。"""

    docid: Series[String] = pa.Field(str_matches=DOCID)  # 書類管理番号
    statement: Series[String] = pa.Field(isin=["BS", "PL", "CF", "SS", "CI", "report", "notes"])
    role: Series[String]  # presentation のロール（例：rol_ConsolidatedBalanceSheet）
    consolidated_role: Series[Int] = pa.Field(**FLAG)  # 連結のロールか
    key: Series[String] = pa.Field(str_contains=":")  # 科目（接頭辞:要素名）
    element_name: Series[String]
    context_ref: Series[String]  # コンテキスト ID（例：CurrentYearInstant_NonConsolidatedMember）
    data_str: Series[String] = pa.Field(nullable=True)  # 値（xsi:nil は欠損）
    is_nil: Series[Int] = pa.Field(**FLAG)
    balance: Series[String] = pa.Field(nullable=True, isin=["debit", "credit"])  # 科目定義の貸借区分
    decimals: Series[String] = pa.Field(nullable=True)  # -3：千円単位、-6：百万円単位、INF など
    unit: Series[String] = pa.Field(nullable=True)  # JPY・pure・shares など
    period_type: Series[String] = pa.Field(nullable=True, isin=["instant", "duration"])
    is_text_block: Series[Int] = pa.Field(**FLAG)
    is_abstract: Series[Int] = pa.Field(**FLAG)
    period_start: Series[String] = pa.Field(nullable=True, str_matches=DATE)  # duration の期首日
    period_end: Series[String] = pa.Field(nullable=True, str_matches=DATE)  # duration の期末日、instant は時点
    instant_date: Series[String] = pa.Field(nullable=True, str_matches=DATE)  # instant の時点
    dimensions: Series[String] = pa.Field(nullable=True)  # 次元とメンバー（JSON。セグメントなど）
    n_source_facts: Series[Int] = pa.Field(ge=1)  # まとめた元の Fact の数（通常 1）
    label_jp: Series[String] = pa.Field(nullable=True)
    label_jp_long: Series[String] = pa.Field(nullable=True)
    label_en: Series[String] = pa.Field(nullable=True)
    label_en_long: Series[String] = pa.Field(nullable=True)
    label_source: Series[String] = pa.Field(nullable=True, isin=["company", "common"])
    preferred_label: Series[String] = pa.Field(nullable=True)  # 表示に使うラベルの種類（totalLabel など）
    order: Series[Int] = pa.Field(ge=1)  # ロール内の表示順（深さ優先の通し番号）
    order_path: Series[String] = pa.Field(str_matches=r"^\d+(_\d+)*$")  # 親からの並び（例：1_3_2）
    layer: Series[Int] = pa.Field(ge=0)  # 階層の深さ（根が 0）
    parent_key_list: Series[object]  # presentation の親科目（リスト）
    non_consolidated_flg: Series[Int] = pa.Field(**FLAG)  # context が個別
    current_flg: Series[Int] = pa.Field(**FLAG)  # context が当期
    prior_flg: Series[Int] = pa.Field(**FLAG)  # context が前期
    calculated_flg: Series[Int] = pa.Field(**FLAG)  # 計算関係の親科目
    net_flg: Series[Int] = pa.Field(**FLAG)  # 「Net」で終わる計算関係の親科目
    net_src_flg: Series[Int] = pa.Field(**FLAG)  # その子科目
    calc_parent_key: Series[String] = pa.Field(nullable=True)
    calc_weight: Series[float] = pa.Field(nullable=True)
    debit_flg: Series[Int] = pa.Field(**FLAG)  # 借方（資産の増加・利益の減少）
    credit_flg: Series[Int] = pa.Field(**FLAG)  # 貸方（負債純資産の増加・利益の増加）
    accounting_standards: Series[String] = pa.Field(nullable=True)  # 会計基準（Japan GAAP・IFRS・US GAAP）

    @pa.dataframe_check
    def unique_row(cls, df: pd.DataFrame) -> bool:
        """（書類, ロール, 科目, context）で一意。"""
        return not df.duplicated(["docid", "role", "key", "context_ref"]).any()

    @pa.dataframe_check
    def net_implies_calculated(cls, df: pd.DataFrame) -> Series[bool]:
        """net_flg が 1 の科目は計算関係の親。"""
        return (df.net_flg == 0) | (df.calculated_flg == 1)

    class Config:
        strict = True
        coerce = True


class FsBigTableDf(FsTblDf):
    """大福帳（One Big Table）：複数書類の fs_tbl に、書類一覧の列（提出者名・業種など）を加えたもの。"""

    filerName: Optional[Series[String]] = pa.Field(nullable=True)  # 提出者名
    sector_label_33: Optional[Series[String]] = pa.Field(nullable=True)  # 東証33業種区分

    class Config:
        strict = False  # 書類一覧の他の列も付けられる
        coerce = True


class NumericFsTblDf(pa.DataFrameModel):
    """``preproc_num`` が加える列。"""

    data: Series[float] = pa.Field(nullable=True)  # data_str を数値に直したもの（直せなければ欠損）
    context_ref_len: Series[int] = pa.Field(ge=1)  # context_ref を "_" で分けた要素の数（少ないほど基本の値）

    class Config:
        strict = False
        coerce = True


class ProcessingLogDf(pa.DataFrameModel):
    """``build_fs_table`` の書類ごとの処理結果。"""

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
    """``download_documents`` の書類ごとの結果。"""

    docid: Series[String]
    status: Series[String] = pa.Field(isin=["success", "skipped", "failure"])
    path: Series[String] = pa.Field(nullable=True)
    bytes: Series[Int] = pa.Field(nullable=True, ge=0)
    message: Series[String] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
