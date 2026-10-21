"""数値・テキストの前処理と LLM の補助。"""

import random

import numpy as np
import pandas as pd
import pytest

from edinet_xbrl_prep.llm import Prompt, ResponseGenAI, extract_json_objects
from edinet_xbrl_prep.numeric import fill_df, pick_base_context, preproc_num
from edinet_xbrl_prep.text import preproc_nlp, reduce_blank_lines


def _rows(rows):
    return pd.DataFrame(rows, columns=["docid", "key", "context_ref", "data_str", "decimals", "label_jp", "filerName"])


@pytest.mark.target("numeric.preproc_num", "numeric.pick_base_context")
def test_preproc_num_and_base_context():
    """data_str を数値に（直せなければ欠損）、context の要素数を数え、要素数の最も少ない行を残す。"""
    data = preproc_num(_rows([
        ("S1", "a:Sales", "CurrentYearDuration", "1000", "-3", "売上高", "A社"),
        ("S1", "a:Sales", "CurrentYearDuration_SegmentMember", "400", "-3", "売上高", "A社"),
        ("S1", "a:Text", "FilingDateInstant", "本文", None, "本文", "A社"),
    ]))
    assert data.data.tolist()[:2] == [1000.0, 400.0] and np.isnan(data.data.iloc[2])
    assert data.context_ref_len.tolist() == [1, 2, 1]
    base = pick_base_context(data)
    assert base[base.key == "a:Sales"].data.tolist() == [1000.0]


@pytest.mark.target("numeric.fill_df")
def test_fill_df_rules():
    """科目がない組み合わせ → 0、値が 0 → 表示単位の半分。補完は同じ書類・同じ科目の中だけ。"""
    data = preproc_num(_rows([
        ("S1", "a:Inventory", "CurrentYearInstant", "500", "-6", "棚卸資産", "A社"),
        ("S1", "a:WIP", "CurrentYearInstant", "0", "-6", "仕掛品", "A社"),
        ("S2", "a:WIP", "CurrentYearInstant", "30", "-3", "仕掛品", "B社"),
    ]))
    filled = fill_df(data).set_index(["docid", "key"])
    assert filled.loc[("S1", "a:WIP"), "data"] == pytest.approx(0.5e6)
    assert filled.loc[("S2", "a:Inventory"), "data"] == 0.0
    # S2 の補完行は S2 の情報（B社・-3）で埋まり、S1 の値を引き継がない
    assert filled.loc[("S2", "a:Inventory"), "filerName"] == "B社"
    assert filled.loc[("S2", "a:Inventory"), "decimals"] == "-3"
    assert filled.loc[("S2", "a:Inventory"), "label_jp"] == "棚卸資産"
    assert len(filled) == 4


def _rtn_droper_original(text):
    """旧版 text_prep.RtnDroper（比較用にそのまま写したもの）。"""
    replaced = text.replace("\n\n", "\n").replace("\n \n", "\n")
    return replaced if replaced == text else _rtn_droper_original(replaced)


@pytest.mark.target("text.reduce_blank_lines")
def test_reduce_blank_lines_matches_original():
    """旧版の再帰の処理（RtnDroper）と同じ結果（ランダムな文字列 2,000 件）。"""
    rng = random.Random(0)
    for _ in range(2000):
        text = "".join(rng.choice(["\n", " ", "a"]) for _ in range(rng.randint(0, 20)))
        assert reduce_blank_lines(text) == _rtn_droper_original(text), repr(text)


@pytest.mark.target("text.preproc_nlp", "text.htmldrop", "text.dropnumber")
def test_preproc_nlp():
    """HTML を除き、NFKC で正規化し、記号を除き、句点を改行にする。"""
    text = "<p>【事業等のリスク】</p>売上高は１２３億円（前期比５．０％増）です。\n\n\n次の行"
    assert preproc_nlp(text, drop_html=True, drop_number=True, reduce_return=True) == "【事業等のリスク】売上高は億円前期比増です\n次の行"
    assert "123" in preproc_nlp(text)


@pytest.mark.target("llm.Prompt.export", "llm.extract_json_objects", "llm.ResponseGenAI.output_json_validation", "llm.ResponseGenAI.extract_output_json")
def test_llm_helpers():
    """プロンプトの組み立てと、応答からの JSON の取り出し。空の応答は ValueError（旧版は送出していなかった）。"""
    prompt = Prompt(instruction="指示", example="例", constraints_list=["a", "b"], output_format="形式")
    system, user = prompt.export("本文")
    assert system == "指示\n\n#### 注意事項\n * a\n * b\n\n形式\n\n#### 例\n例"
    assert user == "#### 文章\n本文\n\n#### 回答\n"
    out = '```json\n{"事業内容":"倉庫事業","具体的な説明":"なし"}\n{"事業内容":"運送"}\n```'
    assert [d["事業内容"] for d in extract_json_objects(out)] == ["倉庫事業", "運送"]
    with pytest.raises(ValueError):
        extract_json_objects("")
    assert ResponseGenAI(output=out).output_json_validation()
    assert not ResponseGenAI(output="").output_json_validation()
    assert not ResponseGenAI(output='{"a": }').output_json_validation()
    assert extract_json_objects("JSON なし") == []
