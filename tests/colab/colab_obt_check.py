"""Colab（CPU）で、パッケージを入れて大福帳（One Big Table）を作り、読み直せることを確かめる。

手元から次の順で実行する（README の「Colab での確認」）。
    colab new -s xbrlprep
    colab upload -s xbrlprep dist/edinet_xbrl_prep-*.whl /content/edinet_xbrl_prep-0.2.0-py3-none-any.whl
    colab upload -s xbrlprep {書類の ZIP} /content/zip/{docid}.zip   # 書類ごと
    colab exec -s xbrlprep -f tests/colab/colab_obt_check.py --timeout 1800
    colab download -s xbrlprep /content/out/colab_obt_check.json ./tests/results/colab_obt_check.json
    colab stop -s xbrlprep
"""

import glob
import json
import platform
import subprocess
import sys
import time
from pathlib import Path

CONTENT = Path("/content")
started = time.time()
wheel = sorted(glob.glob(str(CONTENT / "edinet_xbrl_prep-*.whl")))[-1]
subprocess.run([sys.executable, "-m", "pip", "install", "-q", wheel], check=True)

import pandas as pd  # noqa: E402

from edinet_xbrl_prep import TaxonomyCache, build_fs_table, read_table  # noqa: E402

zip_dir = CONTENT / "zip"
docids = sorted(p.stem for p in zip_dir.glob("*.zip"))
taxonomy = TaxonomyCache(CONTENT / "taxonomy_cache")  # EDINET から必要な版を取得してキャッシュする
documents = pd.DataFrame({"docID": docids, "filerName": [f"会社{i}" for i in range(len(docids))], "sector_label_33": "テスト"})
out = CONTENT / "out" / "fs_big_table.parquet"
result = build_fs_table(docids, zip_dir, taxonomy, statements=["BS", "PL", "report"], documents=documents, out_path=out)

# 読み直す（別の読み方：pandas と pyarrow）
table = read_table(out)
import pyarrow.parquet as pq  # noqa: E402

arrow_rows = pq.ParquetFile(out).metadata.num_rows
lineage = json.loads(out.with_name("fs_big_table_lin.json").read_text(encoding="utf-8"))
bs = table[(table.statement == "BS") & (table.current_flg == 1) & (table.key == "jppfs_cor:Assets")]
summary = {
    "python": platform.python_version(),
    "pandas": pd.__version__,
    "docids": docids,
    "log": result.log[["docid", "status", "n_rows", "accounting_standards", "message"]].to_dict("records"),
    "rows_written": len(result.table),
    "rows_read_pandas": len(table),
    "rows_read_pyarrow": arrow_rows,
    "columns": list(table.columns),
    "rows_by_statement": table.statement.value_counts().to_dict(),
    "label_jp_missing_rate_bs_pl": float(table[table.statement.isin(["BS", "PL"])].label_jp.isna().mean()),
    "total_assets_current": bs[["docid", "role", "context_ref", "data_str", "label_jp"]].to_dict("records"),
    "lineage_keys": list(lineage),
    "lineage_record_num": lineage["record_num"],
    "taxonomy_versions_cached": taxonomy.versions("jpcrp"),
    "seconds": round(time.time() - started, 1),
}
summary["ok"] = (
    len(table) == arrow_rows == lineage["record_num"] == len(result.table) > 0
    and all(r["status"] == "success" for r in summary["log"])
)
out.parent.joinpath("colab_obt_check.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2, default=str)[:4000])
