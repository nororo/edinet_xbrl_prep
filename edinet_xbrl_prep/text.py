"""Preprocessing of text blocks (business risks, description of business, etc.)."""

from __future__ import annotations

import re
import string
import unicodedata

HTML_TAG = re.compile(r"<.*?>")
ASCII_SYMBOLS = re.compile(r"[!-/:-@\[-`{-~]")
EXTRA_SYMBOLS = str.maketrans("", "", string.punctuation + "◆■※")
BLANK_LINES = re.compile(r"\n(?: ?\n)+")


def htmldrop(text: str) -> str:
    """Remove HTML tags."""
    return HTML_TAG.sub("", text)


def dropnumber(text: str) -> str:
    """Remove runs of digits."""
    return re.sub(r"\d+", "", text)


def reduce_blank_lines(text: str) -> str:
    """Collapse blank lines (repeated ``\\n\\n`` or ``\\n \\n``) into a single newline."""
    return BLANK_LINES.sub("\n", text)


def preproc_nlp(text: str, drop_html: bool = False, drop_number: bool = False, reduce_return: bool = False) -> str:
    """Normalize text.

    1. Remove HTML tags (``drop_html``)
    2. Unicode normalization (NFKC: full-width letters, digits and symbols to half-width)
    3. Remove digits (``drop_number``)
    4. Remove ASCII symbols (including ``.``, ``%`` and brackets) and ``◆■※``, and turn the Japanese
       full stop "。" into a newline
    5. Collapse blank lines (``reduce_return``)
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
