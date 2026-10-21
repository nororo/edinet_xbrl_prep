"""Parsing of linkbases (presentation, calculation, label) and schemas.

The same functions turn both document linkbases and common taxonomy linkbases into tables.
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
    """Convert an element ID (prefix_ElementName) to a key (prefix:ElementName).

    Element names contain no ``_``, so the last ``_`` becomes ``:``.

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
    """Label role URI -> name.

    Standard XBRL roles become their last segment (``label``, ``verboseLabel``, etc.). EDINET industry-specific
    roles (such as ``http://disclosure.edinet-fsa.go.jp/jppfs/cns/role/label``) keep the part after the host,
    e.g. ``jppfs/cns/role/label``, to tell them apart from the standard ones.
    """
    if role_uri.startswith("http://www.xbrl.org/"):
        return role_uri.rsplit("/", 1)[-1]
    return role_uri.split("://", 1)[-1].split("/", 1)[-1]


def _locators(link: etree._Element) -> dict[str, str]:
    """Locators of one extended link: reference name (xlink:label) -> key."""
    return {
        loc.get(_x("label")): key_from_href(loc.get(_x("href")))
        for loc in link.iter(f"{{{LINK}}}loc")
    }


def _arcs(data: bytes, link_name: str, arc_name: str) -> list[dict]:
    """Read the arcs of each extended link and return the role, parent/child keys, order and arc attributes."""
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
    """Presentation linkbase -> columns role, parent_key, child_key, order, preferred_label."""
    rows = [
        {
            **{k: r[k] for k in ("role", "parent_key", "child_key", "order")},
            # Only the label role name (the industry-specific jppfs/Consolidated/role/totalLabel also becomes totalLabel)
            "preferred_label": r["attrib"]["preferredLabel"].rsplit("/", 1)[-1]
            if r["attrib"].get("preferredLabel")
            else None,
        }
        for r in _arcs(data, "presentationLink", "presentationArc")
    ]
    return pd.DataFrame(rows, columns=PRESENTATION_COLUMNS)


def parse_calculation(data: bytes) -> pd.DataFrame:
    """Calculation linkbase -> columns role, parent_key, child_key, order, weight.

    When the same parent-child pair appears more than once in a role, the first one is kept.
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
    """Label linkbase -> columns key, label_role, lang, text (one row = account x label role x language)."""
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
    """EDINET taxonomy modules imported by a document schema and their version dates (e.g. {"jppfs": "2023-12-01"})."""
    out: dict[str, str] = {}
    for node in _parse(data).iter(f"{{{XSD}}}import"):
        parts = (node.get("schemaLocation") or "").split("/taxonomy/", 1)
        if len(parts) == 2:
            module, version = parts[1].split("/")[:2]
            out[module] = version
    return out
