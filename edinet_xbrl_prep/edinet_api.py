"""Fetching document lists and documents with the EDINET API (version 2), and the document list table."""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from edinet_xbrl_prep.lineage import save_table
from edinet_xbrl_prep.schemas import DocumentListDf, DownloadLogDf

API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
ENV_API_KEY = "EDINET_API_KEY"
RETRY_STATUS = (429, 500, 502, 503, 504)
# TSE list of listed companies (with industry classifications)
TSE_LISTED_COMPANIES_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

# report kind -> (ordinance code, form code, document type code)
REPORT_KINDS = {
    "annual": ("010", "030000", "120"),  # annual securities report
    "amended_annual": ("010", "030001", "130"),  # amended annual securities report
}
DOCUMENT_COLUMNS = [c for c in DocumentListDf.to_schema().columns if not c.startswith("sector_")]


class ApiKeyMissingError(RuntimeError):
    """No API key in the argument or the environment variable."""


def resolve_api_key(api_key: str | None = None) -> str:
    key = (api_key or os.environ.get(ENV_API_KEY, "")).strip()
    if not key:
        raise ApiKeyMissingError(f"API キーを引数か環境変数 {ENV_API_KEY} で渡すこと")
    return key


def redact(text: str, secret: str | None) -> str:
    return text.replace(secret, "***") if secret else text


def build_session(retries: int = 5, backoff: float = 1.0) -> requests.Session:
    """Session with retries on HTTPS (429, 5xx, connection errors, timeouts)."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=RETRY_STATUS,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def date_range(start: str | dt.date, end: str | dt.date) -> Iterator[dt.date]:
    """Dates from start to end (end inclusive)."""
    start, end = dt.date.fromisoformat(str(start)), dt.date.fromisoformat(str(end))
    if end < start:
        raise ValueError(f"終了日 {end} が開始日 {start} より前")
    for offset in range((end - start).days + 1):
        yield start + dt.timedelta(days=offset)


# ---------------------------------------------------------------- document list API


@dataclass
class DayResult:
    """Result of the document list API for one day."""

    date: str
    status: Literal["success", "failure"]
    access_date: str
    results: list[dict[str, Any]] = field(default_factory=list)
    message: str | None = None


def fetch_document_list(
    date: str | dt.date, *, api_key: str | None = None, session: requests.Session | None = None
) -> DayResult:
    """Fetch the document list of one day (type=2: list and metadata). Failures are recorded in the result, not raised."""
    key = resolve_api_key(api_key)
    session = session or build_session()
    day = str(date)
    access_date = dt.date.today().isoformat()
    try:
        response = session.get(
            f"{API_BASE}/documents.json",
            params={"date": day, "type": 2, "Subscription-Key": key},
            timeout=(20, 60),
        )
        if response.status_code != 200:
            return DayResult(day, "failure", access_date, message=f"HTTP {response.status_code}")
        body = response.json()
        status = str(body.get("metadata", {}).get("status", "200"))
        if status != "200":
            return DayResult(day, "failure", access_date, message=f"API status {status}: {body.get('metadata', {}).get('message')}")
        return DayResult(day, "success", access_date, results=list(body.get("results") or []))
    except (requests.RequestException, json.JSONDecodeError) as exc:
        return DayResult(day, "failure", access_date, message=redact(f"{type(exc).__name__}: {exc}", key))


def fetch_document_lists(
    start_date: str | dt.date,
    end_date: str | dt.date,
    *,
    api_key: str | None = None,
    sleep_sec: float = 0.5,
) -> list[DayResult]:
    """Fetch the document lists day by day from the start date to the end date (both inclusive)."""
    key = resolve_api_key(api_key)
    session = build_session()
    results = []
    for day in tqdm(list(date_range(start_date, end_date)), desc="documents.json"):
        results.append(fetch_document_list(day, api_key=key, session=session))
        time.sleep(sleep_sec)
    return results


def save_day_results(results: Iterable[DayResult], path: str | Path) -> Path:
    """Save the results as they are in JSON Lines (one line = one day), as a record of the API responses."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for result in results:
            f.write(json.dumps(asdict(result), ensure_ascii=False) + "\n")
    return path


def load_day_results(path: str | Path) -> list[DayResult]:
    with Path(path).open(encoding="utf-8") as f:
        return [DayResult(**json.loads(line)) for line in f if line.strip()]


def documents_frame(results: Iterable[DayResult]) -> pd.DataFrame:
    """Turn the lists of successful days into a table with one row per document (``DocumentListDf``)."""
    rows = [
        {**{c: item.get(c) for c in DOCUMENT_COLUMNS}, "date": r.date, "access_date": r.access_date}
        for r in results
        if r.status == "success"
        for item in r.results
    ]
    frame = pd.DataFrame(rows, columns=DOCUMENT_COLUMNS)
    frame = frame.replace({"": None})  # treat empty strings from the API as missing
    return DocumentListDf.validate(frame.sort_values(["date", "seqNumber"], ignore_index=True))


def failed_days(results: Iterable[DayResult]) -> list[str]:
    """Days whose retrieval failed (for retrying)."""
    return [r.date for r in results if r.status != "success"]


def select_reports(documents: pd.DataFrame, kinds: Iterable[str] = ("annual",)) -> pd.DataFrame:
    """Select annual securities reports (``annual``) and amended ones (``amended_annual``) from the document list.

    Rows related to withdrawal (``withdrawalStatus`` other than 0) and the pre-correction rows of documents
    corrected by Local Finance Bureau staff (``docInfoEditStatus`` = 2) are excluded.
    """
    selected = pd.Series(False, index=documents.index)
    for kind in kinds:
        ordinance, form, doc_type = REPORT_KINDS[kind]
        selected |= (
            (documents.ordinanceCode == ordinance)
            & (documents.formCode == form)
            & (documents.docTypeCode == doc_type)
        )
    selected &= (documents.withdrawalStatus == "0") & (documents.docInfoEditStatus != "2")
    return documents[selected].reset_index(drop=True)


def read_tse_sector(source: str | Path = TSE_LISTED_COMPANIES_URL) -> pd.DataFrame:
    """Read securities codes (5 digits) and industry classifications from the TSE list (URL or local .xls)."""
    table = pd.read_excel(source, header=0, dtype=str)
    table = table.rename(
        columns={
            "コード": "secCode",
            "33業種コード": "sector_code_33",
            "33業種区分": "sector_label_33",
            "17業種コード": "sector_code_17",
            "17業種区分": "sector_label_17",
        }
    )
    table["secCode"] = table.secCode.str.ljust(5, "0")
    columns = ["secCode", "sector_code_33", "sector_label_33", "sector_code_17", "sector_label_17"]
    return table[columns].drop_duplicates("secCode")


def add_tse_sector(documents: pd.DataFrame, sector: pd.DataFrame) -> pd.DataFrame:
    """Add industry classification columns to the document list (``sector`` is the result of ``read_tse_sector``).

    The TSE list reflects listed companies and industries at the time of retrieval, so delisted companies
    and the like get no classification.
    """
    merged = documents.drop(columns=[c for c in sector.columns if c != "secCode" and c in documents]).merge(
        sector, on="secCode", how="left"
    )
    return DocumentListDf.validate(merged)


def save_document_list(
    documents: pd.DataFrame, path: str | Path, *, source_data_list: Iterable[str], purpose: str | None = None
) -> Path:
    """Save the document list table with its lineage."""
    dates = documents.date
    return save_table(
        documents,
        path,
        schema=DocumentListDf,
        purpose=purpose or "EDINET 書類一覧 API から得た提出書類の一覧（書類の選択・ダウンロード・大福帳への情報付与に使う）",
        source_code=save_document_list,
        source_data_list=list(source_data_list),
        data_structure=(
            "1行＝1書類（書類一覧 API の1件）。主キー：date, docID。"
            f"提出日の範囲：{dates.min() if len(dates) else '-'}〜{dates.max() if len(dates) else '-'}"
        ),
        importance_columns={
            "docID": "書類管理番号（書類取得 API・ZIP のファイル名）",
            "docTypeCode / ordinanceCode / formCode": "書類の種類（120/010/030000 が有価証券報告書）",
            "edinetCode / secCode": "提出者の EDINET コード・証券コード",
            "periodEnd": "対象期間の終了日（決算日）",
            "submitDateTime": "提出日時",
            "sector_label_33": "東証33業種区分（add_tse_sector で付けた場合）",
        },
    )


# ---------------------------------------------------------------- document API


def download_document(
    docid: str,
    out_path: str | Path,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    doc_type: Literal[1, 2, 5] = 1,
) -> dict[str, Any]:
    """Download one document (type 1: ZIP with XBRL, 2: PDF, 5: CSV) and return the result as a dict."""
    key = resolve_api_key(api_key)
    session = session or build_session()
    out_path = Path(out_path)
    row: dict[str, Any] = {"docid": docid, "status": "failure", "path": None, "bytes": None, "message": None}
    try:
        with session.get(
            f"{API_BASE}/documents/{docid}",
            params={"type": doc_type, "Subscription-Key": key},
            timeout=(20, 120),
            stream=True,
        ) as response:
            content_type = response.headers.get("Content-Type", "")
            if response.status_code != 200 or "json" in content_type:
                row["message"] = f"HTTP {response.status_code} {content_type}: {response.text[:200]}"
                return row
            out_path.parent.mkdir(parents=True, exist_ok=True)
            partial = out_path.with_suffix(out_path.suffix + ".part")
            with partial.open("wb") as f:
                for chunk in response.iter_content(1 << 16):
                    f.write(chunk)
            partial.rename(out_path)
        row.update(status="success", path=str(out_path), bytes=out_path.stat().st_size)
    except requests.RequestException as exc:
        row["message"] = redact(f"{type(exc).__name__}: {exc}", key)
    return row


def download_documents(
    docids: Iterable[str],
    out_dir: str | Path,
    *,
    api_key: str | None = None,
    sleep_sec: float = 0.5,
    overwrite: bool = False,
) -> pd.DataFrame:
    """Download document ZIPs to ``{out_dir}/{docid}.zip``. Existing files are skipped (``skipped``)."""
    key = resolve_api_key(api_key)
    session = build_session()
    out_dir = Path(out_dir)
    rows = []
    for docid in tqdm(list(dict.fromkeys(docids)), desc="documents"):
        target = out_dir / f"{docid}.zip"
        if target.exists() and not overwrite:
            rows.append({"docid": docid, "status": "skipped", "path": str(target), "bytes": target.stat().st_size, "message": None})
            continue
        rows.append(download_document(docid, target, api_key=key, session=session))
        time.sleep(sleep_sec)
    return DownloadLogDf.validate(pd.DataFrame(rows, columns=list(DownloadLogDf.to_schema().columns)))
