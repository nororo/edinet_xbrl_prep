"""EDINET API（通信はモックに置き換える）と書類一覧の表。"""

import json

import pandas as pd
import pytest
import requests

from edinet_xbrl_prep import edinet_api
from edinet_xbrl_prep.edinet_api import DayResult
from edinet_xbrl_prep.schemas import DocumentListDf

KEY = "secret-key-123"


def _doc(docid, seq, doc_type="120", form="030000", withdrawal="0", edit="0", sec="72030"):
    item = dict.fromkeys(edinet_api.DOCUMENT_COLUMNS)
    item.update(
        seqNumber=seq, docID=docid, edinetCode="E00001", secCode=sec, filerName="テスト株式会社",
        ordinanceCode="010", formCode=form, docTypeCode=doc_type, periodEnd="2024-03-31",
        submitDateTime="2024-06-20 15:00", withdrawalStatus=withdrawal, docInfoEditStatus=edit,
        disclosureStatus="0", xbrlFlag="1", pdfFlag="1", attachDocFlag="0", englishDocFlag="0",
        csvFlag="1", legalStatus="1", fundCode="",
    )
    del item["date"], item["access_date"]
    return item


class FakeResponse:
    def __init__(self, status=200, body=None, content=b"", content_type="application/json"):
        self.status_code = status
        self._body = body
        self._content = content
        self.headers = {"Content-Type": content_type}
        self.text = json.dumps(body) if body is not None else ""

    def json(self):
        return self._body

    def iter_content(self, size):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.calls = response, error, []

    def get(self, url, params=None, **kwargs):
        self.calls.append((url, params, kwargs))
        if self.error:
            raise self.error
        return self.response


@pytest.mark.target("edinet_api.date_range")
def test_date_range_includes_end():
    """終了日を含む（参考プロジェクトの review H5）。"""
    days = list(edinet_api.date_range("2024-06-29", "2024-07-01"))
    assert [str(d) for d in days] == ["2024-06-29", "2024-06-30", "2024-07-01"]
    with pytest.raises(ValueError):
        list(edinet_api.date_range("2024-07-01", "2024-06-30"))


@pytest.mark.target("edinet_api.resolve_api_key")
def test_api_key_from_argument_or_env(monkeypatch):
    """API キーは引数、なければ環境変数 EDINET_API_KEY。どちらもなければエラー。"""
    monkeypatch.delenv("EDINET_API_KEY", raising=False)
    with pytest.raises(edinet_api.ApiKeyMissingError):
        edinet_api.resolve_api_key()
    monkeypatch.setenv("EDINET_API_KEY", "from-env")
    assert edinet_api.resolve_api_key() == "from-env"
    assert edinet_api.resolve_api_key("arg") == "arg"


@pytest.mark.target("edinet_api.build_session")
def test_session_retries_on_https():
    """リトライは https:// にも設定する（旧版は http:// だけだった）。"""
    adapter = edinet_api.build_session().get_adapter("https://api.edinet-fsa.go.jp")
    assert adapter.max_retries.total == 5
    assert 429 in adapter.max_retries.status_forcelist


@pytest.mark.target("edinet_api.fetch_document_list")
def test_fetch_document_list_success_and_tls():
    """成功時は results を持ち、TLS の検証を無効にしない（verify を渡さない）。"""
    session = FakeSession(FakeResponse(body={"metadata": {"status": "200"}, "results": [_doc("S1000001", 1)]}))
    result = edinet_api.fetch_document_list("2024-06-20", api_key=KEY, session=session)
    assert result.status == "success" and len(result.results) == 1
    url, params, kwargs = session.calls[0]
    assert params["type"] == 2 and params["date"] == "2024-06-20"
    assert "verify" not in kwargs


@pytest.mark.target("edinet_api.fetch_document_list")
def test_fetch_document_list_failures_are_recorded_without_key():
    """HTTP エラー・API のエラー・通信例外は failure として記録し、メッセージに API キーを含めない。"""
    http = edinet_api.fetch_document_list("2024-06-20", api_key=KEY, session=FakeSession(FakeResponse(status=500)))
    api = edinet_api.fetch_document_list(
        "2024-06-20", api_key=KEY,
        session=FakeSession(FakeResponse(body={"metadata": {"status": "401", "message": "Access denied"}})),
    )
    error = edinet_api.fetch_document_list(
        "2024-06-20", api_key=KEY, session=FakeSession(error=requests.ConnectionError(f"url?key={KEY}"))
    )
    assert [r.status for r in (http, api, error)] == ["failure"] * 3
    assert "401" in api.message
    assert KEY not in error.message and "***" in error.message


@pytest.mark.target("edinet_api.documents_frame", "edinet_api.save_day_results", "edinet_api.load_day_results", "schemas.DocumentListDf.unique_document")
def test_documents_frame_roundtrip(tmp_path):
    """成功した日の書類を1行ずつの表にする（空文字は欠損）。JSON Lines に保存して読み直せる。"""
    results = [
        DayResult("2024-06-20", "success", "2024-07-01", [_doc("S1000002", 2), _doc("S1000001", 1)]),
        DayResult("2024-06-21", "failure", "2024-07-01", message="HTTP 500"),
    ]
    path = edinet_api.save_day_results(results, tmp_path / "days.jsonl")
    loaded = edinet_api.load_day_results(path)
    frame = edinet_api.documents_frame(loaded)
    assert frame.docID.tolist() == ["S1000001", "S1000002"]
    assert frame.fundCode.isna().all()
    assert edinet_api.failed_days(loaded) == ["2024-06-21"]
    DocumentListDf.validate(frame)
    with pytest.raises(Exception, match="unique_document"):
        DocumentListDf.validate(pd.concat([frame, frame]))


@pytest.mark.target("edinet_api.select_reports")
def test_select_reports():
    """有価証券報告書（120/010/030000）を選び、取下げ・修正前の行を除く。訂正報告書も選べる。"""
    docs = [
        _doc("S1000001", 1), _doc("S1000002", 2, doc_type="130", form="030001"),
        _doc("S1000003", 3, withdrawal="2"), _doc("S1000004", 4, edit="2"),
        _doc("S1000005", 5, doc_type="140", form="043000"),
    ]
    frame = edinet_api.documents_frame([DayResult("2024-06-20", "success", "2024-07-01", docs)])
    assert edinet_api.select_reports(frame).docID.tolist() == ["S1000001"]
    both = edinet_api.select_reports(frame, ["annual", "amended_annual"])
    assert both.docID.tolist() == ["S1000001", "S1000002"]


@pytest.mark.target("edinet_api.read_tse_sector", "edinet_api.add_tse_sector")
def test_add_tse_sector(tmp_path, monkeypatch):
    """東証の一覧の4桁コードを5桁にそろえて結合する。一覧にない会社は欠損。"""
    sheet = pd.DataFrame({"日付": ["20240531"] * 2, "コード": ["7203", "9999"], "33業種コード": ["3700", "1"],
                          "33業種区分": ["輸送用機器", "x"], "17業種コード": ["6", "1"], "17業種区分": ["自動車・輸送機", "x"]})
    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: sheet.astype(str))
    sector = edinet_api.read_tse_sector("dummy.xls")
    docs = [_doc("S1000001", 1), _doc("S1000002", 2, sec="13010")]
    frame = edinet_api.add_tse_sector(
        edinet_api.documents_frame([DayResult("2024-06-20", "success", "2024-07-01", docs)]), sector
    )
    labels = frame.set_index("docID").sector_label_33
    assert labels["S1000001"] == "輸送用機器"
    assert pd.isna(labels["S1000002"])


@pytest.mark.target("edinet_api.download_document")
def test_download_document(tmp_path):
    """ZIP を保存する。JSON（エラー）が返ったら failure。"""
    ok = edinet_api.download_document(
        "S1000001", tmp_path / "S1000001.zip", api_key=KEY,
        session=FakeSession(FakeResponse(content=b"PK..", content_type="application/octet-stream")),
    )
    assert ok["status"] == "success" and (tmp_path / "S1000001.zip").read_bytes() == b"PK.."
    ng = edinet_api.download_document(
        "S1000002", tmp_path / "S1000002.zip", api_key=KEY,
        session=FakeSession(FakeResponse(body={"metadata": {"status": "404"}})),
    )
    assert ng["status"] == "failure" and not (tmp_path / "S1000002.zip").exists()


@pytest.mark.target("edinet_api.download_documents")
def test_download_documents_skips_existing(tmp_path, monkeypatch):
    """すでにある ZIP は取り直さない。"""
    (tmp_path / "S1000001.zip").write_bytes(b"PK")
    monkeypatch.setattr(edinet_api, "download_document", lambda *a, **k: pytest.fail("呼ばれない"))
    log = edinet_api.download_documents(["S1000001"], tmp_path, api_key=KEY, sleep_sec=0)
    assert log.status.tolist() == ["skipped"]


@pytest.mark.target("edinet_api.fetch_document_lists", "edinet_api.failed_days")
def test_fetch_document_lists_covers_each_day(monkeypatch):
    """期間の各日（終了日を含む）を1回ずつ取得する。"""
    called = []

    def fake(day, **kwargs):
        called.append(str(day))
        return DayResult(str(day), "failure" if str(day).endswith("02") else "success", "2024-07-03")

    monkeypatch.setattr(edinet_api, "fetch_document_list", fake)
    results = edinet_api.fetch_document_lists("2024-07-01", "2024-07-03", api_key=KEY, sleep_sec=0)
    assert called == ["2024-07-01", "2024-07-02", "2024-07-03"]
    assert edinet_api.failed_days(results) == ["2024-07-02"]


@pytest.mark.target("edinet_api.save_document_list", "edinet_api.redact")
def test_save_document_list_with_lineage(tmp_path):
    """書類一覧をリネージ付きで保存する。"""
    frame = edinet_api.documents_frame([DayResult("2024-06-20", "success", "2024-07-01", [_doc("S1000001", 1)])])
    lin = edinet_api.save_document_list(frame, tmp_path / "documents.parquet", source_data_list=["EDINET 書類一覧 API"])
    content = json.loads(lin.read_text(encoding="utf-8"))
    assert content["record_num"] == 1 and "2024-06-20" in content["data_structure"]
    assert edinet_api.redact(f"x{KEY}y", KEY) == "x***y"
