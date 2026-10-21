# %% [markdown]
# # 有価証券報告書 XBRL の分析例（v0.2 の API）
# `xbrl_analysis.ipynb`（v0.1）と同じ流れを、v0.2 の関数で書いたもの。VS Code・Jupyter では `# %%` ごとに実行できる。
# 分析の部分（4・5）は追加の依存が必要：`pip install "edinet-xbrl-prep[analysis,llm]"`

# %%
# 0. 設定
import getpass
from pathlib import Path

import pandas as pd

from edinet_xbrl_prep import (
    TaxonomyCache,
    add_tse_sector,
    build_fs_table,
    documents_frame,
    download_documents,
    fetch_document_lists,
    fill_df,
    pick_base_context,
    preproc_nlp,
    preproc_num,
    read_tse_sector,
    save_day_results,
    save_document_list,
    select_reports,
)

DATA_PATH = Path("../data")  # ダウンロードしたデータなどを置く場所
API_KEY = getpass.getpass("EDINET API キー: ")  # 環境変数 EDINET_API_KEY があれば None でもよい

# %%
# 1. 提出書類一覧の取得（終了日を含む）
results = fetch_document_lists("2024-06-15", "2024-06-30", api_key=API_KEY)
save_day_results(results, DATA_PATH / "document_list_days.jsonl")  # API の応答の控え
documents = documents_frame(results)

# 有価証券報告書を選び、東証の業種区分を付ける
yuho = add_tse_sector(select_reports(documents), read_tse_sector())
save_document_list(
    yuho,
    DATA_PATH / "yuho_documents.parquet",  # yuho_documents_lin.json（リネージ）も書く
    source_data_list=["EDINET 書類一覧 API（2024-06-15〜2024-06-30）", "東証 上場銘柄一覧（data_j.xls）"],
)
target = yuho.query("sector_label_33 == '食料品'").head(30)
print("業種が食料品の有価証券報告書数:", len(target))

# %%
# 2. 有価証券報告書のダウンロード（すでにある ZIP は飛ばす）
download_log = download_documents(target.docID, DATA_PATH / "raw/xbrl_doc", api_key=API_KEY)
print(download_log.status.value_counts())

# %%
# 3. 大福帳（One Big Table）の作成
# タクソノミは、書類が参照する版を EDINET から取得してキャッシュする（v0.1 の account_list_common は不要）
taxonomy = TaxonomyCache(DATA_PATH / "taxonomy_cache")
result = build_fs_table(
    target.docID,
    DATA_PATH / "raw/xbrl_doc",
    taxonomy,
    statements=["BS", "PL", "report"],  # v0.1 の role_keyward_list=fs_dict['BS']+fs_dict['PL']+fs_dict['report']
    documents=target,  # filerName・sector_label_33 を付ける
    out_path=DATA_PATH / "fs_big_table.parquet",  # 大福帳・処理結果・それぞれのリネージを保存
)
fs_tbl_df_all = result.table
print(result.log.status.value_counts())
fs_tbl_df_all.head()[["key", "data_str", "decimals", "context_ref", "period_end", "role", "label_jp", "order", "docid", "filerName"]]

# %%
# 4.1 事業等のリスクのテキスト
text_df = fs_tbl_df_all.query("statement == 'report'")
business_risk_df = text_df.query("key == 'jpcrp_cor:BusinessRisksTextBlock'").copy()
business_risk_df["text_processed"] = business_risk_df.data_str.apply(
    preproc_nlp, drop_html=True, drop_number=True, reduce_return=True  # v0.1 の drop_htmp は drop_html に
)

# %%
# 5.1 在庫の構成（v0.1 と同じ規則で補完する）
bs_df = fs_tbl_df_all.query("non_consolidated_flg == 0 and current_flg == 1 and statement == 'BS'")
inventory_df = bs_df[bs_df.label_jp.str.contains("商品及び製品|仕掛品|原材料及び貯蔵品", na=False)]
inventory_df = pick_base_context(preproc_num(inventory_df))  # セグメント別の値を除き、基本の値を残す
inventory_filled = fill_df(inventory_df)
inventory_pivot = pd.crosstab(
    index=inventory_filled.filerName, columns=inventory_filled.label_jp,
    values=inventory_filled.data, aggfunc="sum", normalize="index",
)

# %%
# 5.3 営業利益率と総資産
pl_df = fs_tbl_df_all.query("non_consolidated_flg == 0 and current_flg == 1 and statement == 'PL'")
profit = pick_base_context(preproc_num(pl_df.query("key == 'jppfs_cor:OperatingIncome'"))).set_index("docid").data
sales = pick_base_context(preproc_num(pl_df.query("key == 'jppfs_cor:NetSales'"))).set_index("docid").data
assets = pick_base_context(preproc_num(bs_df.query("key == 'jppfs_cor:Assets'"))).set_index("docid").data
pd.DataFrame({"total_assets": assets / 1e6, "operating_margin": profit / sales}).dropna()
