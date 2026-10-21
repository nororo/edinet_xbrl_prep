"""テキストブロック（事業等のリスク・事業の内容など）の前処理（旧 ``text_prep.py`` の前半）。

旧版との違い:
- ``RtnDroper``（再帰）を ``reduce_blank_lines``（正規表現）にした。結果は同じ
- 引数名の誤記 ``drop_htmp`` を ``drop_html`` に直した
"""

from __future__ import annotations

import re
import string
import unicodedata

HTML_TAG = re.compile(r"<.*?>")
ASCII_SYMBOLS = re.compile(r"[!-/:-@\[-`{-~]")
EXTRA_SYMBOLS = str.maketrans("", "", string.punctuation + "◆■※")
BLANK_LINES = re.compile(r"\n(?: ?\n)+")


def htmldrop(text: str) -> str:
    """HTML のタグを取り除く。"""
    return HTML_TAG.sub("", text)


def dropnumber(text: str) -> str:
    """数字の並びを取り除く。"""
    return re.sub(r"\d+", "", text)


def reduce_blank_lines(text: str) -> str:
    """空行（``\\n\\n``・``\\n \\n`` の繰り返し）を1つの改行にする。"""
    return BLANK_LINES.sub("\n", text)


def preproc_nlp(text: str, drop_html: bool = False, drop_number: bool = False, reduce_return: bool = False) -> str:
    """テキストを正規化する。

    1. HTML のタグを除く（``drop_html``）
    2. Unicode 正規化（NFKC。全角英数字・記号を半角に）
    3. 数字を除く（``drop_number``）
    4. ASCII の記号（``.``・``%``・括弧などを含む）と ``◆■※`` を除き、句点「。」を改行にする
    5. 空行を詰める（``reduce_return``）
    """
    if drop_html:
        text = htmldrop(text)
    text = unicodedata.normalize("NFKC", text)
    if drop_number:
        text = dropnumber(text)
    text = ASCII_SYMBOLS.sub("", text)
    text = text.replace("。", "\n").translate(EXTRA_SYMBOLS)
    if reduce_return:
        text = reduce_blank_lines(text)
    return text
