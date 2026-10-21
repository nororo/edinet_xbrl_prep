"""テストを実行し、テストの内容と対象の関数の対応を HTML（../docs/tests_report.html）にまとめる。

    uv run python scripts/build_tests_report.py
    # 実際の書類を使うテストも含める
    EDINET_FIXTURE_DIR=/path/to/zip uv run python scripts/build_tests_report.py

各テストの ``@pytest.mark.target("モジュール.関数")`` を対象の関数とし、docstring をテストの内容とする。
パッケージの公開関数（依存グラフと同じ解析）のうち、テストの対象になっていないものも一覧にする。
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import sys
from collections import defaultdict
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from build_dependency_graph import DOCS_DIR, PACKAGE, collect  # noqa: E402


class Collector:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def pytest_collection_modifyitems(self, items):
        for item in items:
            targets = []
            for mark in item.iter_markers("target"):
                targets.extend(mark.args)
            doc = (getattr(item.obj, "__doc__", None) or "").strip()
            self.items[item.nodeid] = {
                "nodeid": item.nodeid,
                "file": item.path.name,
                "name": item.name,
                "doc": doc,
                "targets": sorted(set(targets)),
                "markers": sorted({m.name for m in item.iter_markers()} - {"target", "parametrize"}),
                "outcome": "not run",
                "duration": 0.0,
                "message": "",
            }

    def pytest_runtest_logreport(self, report):
        entry = self.items.get(report.nodeid)
        if entry is None:
            return
        if report.when == "call" or (report.when == "setup" and report.outcome != "passed"):
            entry["outcome"] = report.outcome
            entry["duration"] = round(report.duration, 3)
            if report.outcome != "passed":
                entry["message"] = str(report.longrepr)[-600:]


def public_functions() -> list[str]:
    modules = collect(ROOT / PACKAGE)
    return sorted(
        f.id for m in modules.values() if m.name != "__init__"
        for f in m.funcs.values()
        if f.kind != "class" and not f.name.split(".")[-1].startswith("_")
    )


def target_matches(target: str, function: str) -> bool:
    """マーカーの対象（モジュール名だけ・クラス名だけも可）が関数に当たるか。"""
    return function == target or function.startswith(target + ".")


def render(entries: list[dict], functions: list[str], exit_code: int) -> str:
    counts = defaultdict(int)
    for e in entries:
        counts[e["outcome"]] += 1
    by_target = defaultdict(list)
    for e in entries:
        for t in e["targets"]:
            by_target[t].append(e)
    covered = {f for f in functions if any(target_matches(t, f) for e in entries for t in e["targets"] if "." in t)}
    uncovered = [f for f in functions if f not in covered]
    rows = "".join(
        f'<tr class="{e["outcome"]}"><td><code>{html.escape(e["file"])}</code><br><b>{html.escape(e["name"])}</b></td>'
        f'<td>{html.escape(e["doc"]).replace(chr(10), "<br>")}</td>'
        f'<td>{"<br>".join(f"<code>{html.escape(t)}</code>" for t in e["targets"]) or "-"}</td>'
        f'<td>{html.escape(", ".join(e["markers"])) or ""}</td>'
        f'<td class="o">{e["outcome"]}<br><small>{e["duration"]}s</small>'
        + (f'<details><summary>詳細</summary><pre>{html.escape(e["message"])}</pre></details>' if e["message"] else "")
        + "</td></tr>"
        for e in entries
    )
    target_rows = "".join(
        f'<tr><td><code>{html.escape(t)}</code></td><td>{len(es)}</td>'
        f'<td>{"<br>".join(html.escape(e["name"]) for e in es)}</td></tr>'
        for t, es in sorted(by_target.items())
    )
    summary = " / ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
    data = json.dumps({"exit_code": exit_code, "counts": counts}, ensure_ascii=False)
    return f"""<!doctype html><html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>テスト一覧</title>
<style>
:root{{--bg:#fff;--fg:#1f2328;--muted:#656d76;--line:#d0d7de;--ok:#1a7f37;--ng:#cf222e;--skip:#9a6700;--panel:#f6f8fa}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0d1117;--fg:#e6edf3;--muted:#8d96a0;--line:#30363d;--ok:#3fb950;--ng:#f85149;--skip:#d29922;--panel:#161b22}}}}
body{{margin:0 auto;max-width:1200px;padding:16px;font-family:system-ui,-apple-system,"Hiragino Sans",sans-serif;background:var(--bg);color:var(--fg);font-size:14px}}
h1{{font-size:20px}}h2{{font-size:16px;margin-top:28px}}.sub{{color:var(--muted)}}
table{{border-collapse:collapse;width:100%}}th,td{{border-bottom:1px solid var(--line);padding:6px;vertical-align:top;text-align:left}}
th{{background:var(--panel);position:sticky;top:0}}code{{font-size:12px;word-break:break-all}}
tr.passed .o{{color:var(--ok)}}tr.failed .o{{color:var(--ng);font-weight:bold}}tr.skipped .o{{color:var(--skip)}}
input{{padding:4px 8px;width:min(400px,100%);margin:8px 0}}pre{{white-space:pre-wrap;font-size:11px}}
.wrap{{overflow-x:auto}}table.fixed{{table-layout:fixed;min-width:720px}}td{{overflow-wrap:anywhere}}
</style></head><body>
<h1>edinet_xbrl_prep のテスト一覧</h1>
<p class="sub">実行：{dt.datetime.now().astimezone().isoformat(timespec="seconds")}　結果：{summary}　（skipped は実際の書類の ZIP〔EDINET_FIXTURE_DIR〕がない環境で飛ばしたテスト）</p>
<h2>テストと対象の関数</h2>
<input id="q" placeholder="絞り込み（テスト名・内容・関数名）">
<div class="wrap"><table id="tests" class="fixed"><colgroup><col style="width:22%"><col style="width:40%"><col style="width:24%"><col style="width:6%"><col style="width:8%"></colgroup><thead><tr><th>テスト</th><th>内容</th><th>対象の関数</th><th>種類</th><th>結果</th></tr></thead><tbody>{rows}</tbody></table></div>
<h2>関数から見たテスト</h2>
<div class="wrap"><table><thead><tr><th>対象</th><th>件数</th><th>テスト</th></tr></thead><tbody>{target_rows}</tbody></table></div>
<h2>テストの対象になっていない公開の関数・メソッド（{len(uncovered)} / {len(functions)}）</h2>
<p>{"、".join(f"<code>{html.escape(f)}</code>" for f in uncovered) or "なし"}</p>
<script>
const DATA = {data};
document.getElementById('q').addEventListener('input', e => {{
  const q = e.target.value.toLowerCase();
  document.querySelectorAll('#tests tbody tr').forEach(tr => tr.style.display = tr.textContent.toLowerCase().includes(q) ? '' : 'none');
}});
</script></body></html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DOCS_DIR, help="出力先（既定：リポジトリの外の ../docs）")
    args = parser.parse_args()
    collector = Collector()
    exit_code = pytest.main(["-q", "-p", "no:cacheprovider", str(ROOT / "tests")], plugins=[collector])
    entries = sorted(collector.items.values(), key=lambda e: (e["file"], e["nodeid"]))
    out = args.out_dir / "tests_report.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(entries, public_functions(), int(exit_code)), encoding="utf-8")
    print(f"{out}（{len(entries)} 件）")
    return int(exit_code)


if __name__ == "__main__":
    sys.exit(main())
