# %%
import warnings

warnings.filterwarnings("ignore")

from pathlib import Path

# このパスの下にダウンロードしたデータやらいろいろ置きます。
DATA_PATH = Path("../data")
(DATA_PATH / "raw/xbrl_doc").mkdir(
    parents=True,
    exist_ok=True,
)  # ダウンロードしたzipファイルの保管場所
(DATA_PATH / "raw/xbrl_doc_ext").mkdir(
    parents=True,
    exist_ok=True,
)  # zipファイルから抽出したXBRL、スキーマファイル、リンクベースファイルの保管場所

# EDINET APIのAPIキーを入力してください。
your_api_key: str = input("EDINET API keyを入力してください: ")

# %%
