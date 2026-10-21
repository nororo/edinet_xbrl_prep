"""linkbase（presentation・calculation・label）とスキーマの解析。

書類の linkbase と共通タクソノミの linkbase を、同じ関数で表に直す。

旧版（``link_base_file_analyzer.py``）との違い:
- ``get_presentation_account_list``・``get_presentation_common``・``get_calc_edge_list``・``get_label``・
  ``get_label_common`` の5クラスに重複していた解析を、3つの関数にまとめた
- 各ファイルの locator（``xlink:href``）から、そのファイル自身で科目（key）を決める。旧版はラベルの参照名を
  presentation の参照名で引き直していたため、名前が違うと企業別のラベルが付かなかった
- presentation の ``preferredLabel`` を残す。共通タクソノミの presentation の order を ``'1'`` に固定していた点も解消
"""

from __future__ import annotations

from collections import defaultdict

import pandas as pd
from lxml import etree

XLINK = "http://www.w3.org/1999/xlink"
LINK = "http://www.xbrl.org/2003/linkbase"
XSD = "http://www.w3.org/2001/XMLSchema"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
XBRL_ROLE_PREFIX = "http://www.xbrl.org/2003/role/"

PRESENTATION_COLUMNS = ["role", "parent_key", "child_key", "order", "preferred_label"]
CALCULATION_COLUMNS = ["role", "parent_key", "child_key", "order", "weight"]
LABEL_COLUMNS = ["key", "label_role", "lang", "text"]


def _x(name: str) -> str:
    return f"{{{XLINK}}}{name}"


def _parse(data: bytes) -> etree._Element:
    parser = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True)
    return etree.fromstring(data, parser=parser)


def format_taxonomi(taxonomi_str: str) -> str:
    """要素 ID（接頭辞_要素名）を key（接頭辞:要素名）に直す。

    要素名に ``_`` は含まれないので、最後の ``_`` を ``:`` にする。

    >>> format_taxonomi("jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF")
    'jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF'
    """
    if not taxonomi_str:
        raise ValueError("入力文字列が空またはNoneです。有効なタクソノミ文字列を入力してください。")
    if "_" not in taxonomi_str:
        raise ValueError(
            "入力文字列にアンダースコア('_')が含まれていません。適切なタクソノミ文字列を入力してください。"
        )
    prefix, _, name = taxonomi_str.rpartition("_")
    return f"{prefix}:{name}"


def key_from_href(href: str) -> str:
    """``xxx.xsd#jppfs_cor_Assets`` → ``jppfs_cor:Assets``。"""
    return format_taxonomi(href.split("#", 1)[-1])


def role_suffix(role_uri: str) -> str:
    """``http://.../role/jppfs/rol_BalanceSheet`` → ``rol_BalanceSheet``。"""
    return role_uri.rstrip("/").rsplit("/", 1)[-1]


def label_role_name(role_uri: str) -> str:
    """ラベルの種類の URI → 名前。

    XBRL 標準の種類は末尾の名前（``label``・``verboseLabel`` など）。EDINET の業種別の種類
    （``http://disclosure.edinet-fsa.go.jp/jppfs/cns/role/label`` など）は、標準と区別するため
    ``jppfs/cns/role/label`` のようにホスト以降を残す。
    """
    if role_uri.startswith("http://www.xbrl.org/"):
        return role_uri.rsplit("/", 1)[-1]
    return role_uri.split("://", 1)[-1].split("/", 1)[-1]


def _locators(link: etree._Element) -> dict[str, str]:
    """1つの extended link の locator：参照名（xlink:label）→ key。"""
    return {
        loc.get(_x("label")): key_from_href(loc.get(_x("href")))
        for loc in link.iter(f"{{{LINK}}}loc")
    }


def _arcs(data: bytes, link_name: str, arc_name: str) -> list[dict]:
    """extended link ごとに arc を読み、role・親子の key・order と arc の属性を返す。"""
    rows = []
    for link in _parse(data).iter(f"{{{LINK}}}{link_name}"):
        role = role_suffix(link.get(_x("role")))
        locators = _locators(link)
        for arc in link.iter(f"{{{LINK}}}{arc_name}"):
            rows.append(
                {
                    "role": role,
                    "parent_key": locators[arc.get(_x("from"))],
                    "child_key": locators[arc.get(_x("to"))],
                    "order": float(arc.get("order", "0")),
                    "attrib": arc.attrib,
                }
            )
    return rows


def parse_presentation(data: bytes) -> pd.DataFrame:
    """presentation linkbase → 列 role, parent_key, child_key, order, preferred_label。"""
    rows = [
        {
            **{k: r[k] for k in ("role", "parent_key", "child_key", "order")},
            # ラベルの種類の名前だけ（業種別の jppfs/Consolidated/role/totalLabel も totalLabel）
            "preferred_label": r["attrib"]["preferredLabel"].rsplit("/", 1)[-1]
            if r["attrib"].get("preferredLabel")
            else None,
        }
        for r in _arcs(data, "presentationLink", "presentationArc")
    ]
    return pd.DataFrame(rows, columns=PRESENTATION_COLUMNS)


def parse_calculation(data: bytes) -> pd.DataFrame:
    """calculation linkbase → 列 role, parent_key, child_key, order, weight。

    同じロールに同じ親子の組が複数あるときは、最初の1つを残す（旧版と同じ）。
    """
    rows = [
        {
            **{k: r[k] for k in ("role", "parent_key", "child_key", "order")},
            "weight": float(r["attrib"].get("weight", "1")),
        }
        for r in _arcs(data, "calculationLink", "calculationArc")
    ]
    frame = pd.DataFrame(rows, columns=CALCULATION_COLUMNS)
    return frame.drop_duplicates(subset=["role", "parent_key", "child_key"], ignore_index=True)


def parse_labels(data: bytes) -> pd.DataFrame:
    """label linkbase → 列 key, label_role, lang, text（1行＝1科目×1種類×1言語）。"""
    rows = []
    for link in _parse(data).iter(f"{{{LINK}}}labelLink"):
        locators = _locators(link)
        resources: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        for label in link.iter(f"{{{LINK}}}label"):
            role = label.get(_x("role"), XBRL_ROLE_PREFIX + "label")
            resources[label.get(_x("label"))].append(
                (label_role_name(role), label.get(XML_LANG), "".join(label.itertext()).strip())
            )
        for arc in link.iter(f"{{{LINK}}}labelArc"):
            key = locators.get(arc.get(_x("from")))
            if key is None:
                continue
            for label_role, lang, text in resources.get(arc.get(_x("to")), []):
                rows.append({"key": key, "label_role": label_role, "lang": lang, "text": text})
    return pd.DataFrame(rows, columns=LABEL_COLUMNS).drop_duplicates(ignore_index=True)


def schema_imports(data: bytes) -> dict[str, str]:
    """書類のスキーマが import する EDINET タクソノミのモジュールと版の日付（例：{"jppfs": "2023-12-01"}）。"""
    out: dict[str, str] = {}
    for node in _parse(data).iter(f"{{{XSD}}}import"):
        parts = (node.get("schemaLocation") or "").split("/taxonomy/", 1)
        if len(parts) == 2:
            module, version = parts[1].split("/")[:2]
            out[module] = version
    return out
