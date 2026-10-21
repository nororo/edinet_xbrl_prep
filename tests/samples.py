"""テスト用の小さな linkbase・スキーマ（実際の EDINET の書類と同じ書き方）。"""

from __future__ import annotations

HEAD = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" '
    'xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xml="http://www.w3.org/XML/1998/namespace">'
)
TAIL = "</link:linkbase>"
XSD = "jppfs_cor_2023-12-01.xsd"
ROLE = "http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_ConsolidatedBalanceSheet"


def loc(label: str, element_id: str, xsd: str = XSD) -> str:
    return f'<link:loc xlink:type="locator" xlink:href="{xsd}#{element_id}" xlink:label="{label}"/>'


def presentation(arcs: list[tuple[str, str, float, str | None]], role: str = ROLE) -> bytes:
    """arcs: (親の要素 ID, 子の要素 ID, order, preferredLabel の末尾)。参照名は要素 ID に "loc_" を付けたもの。"""
    ids = dict.fromkeys(i for arc in arcs for i in arc[:2])
    body = "".join(loc(f"loc_{i}", i) for i in ids)
    for parent, child, order, preferred in arcs:
        pl = f' preferredLabel="http://www.xbrl.org/2003/role/{preferred}"' if preferred else ""
        body += (
            '<link:presentationArc xlink:type="arc" '
            'xlink:arcrole="http://www.xbrl.org/2003/arcrole/parent-child" '
            f'xlink:from="loc_{parent}" xlink:to="loc_{child}" order="{order}"{pl}/>'
        )
    return f'{HEAD}<link:presentationLink xlink:type="extended" xlink:role="{role}">{body}</link:presentationLink>{TAIL}'.encode()


def calculation(arcs: list[tuple[str, str, float]], role: str = ROLE) -> bytes:
    """arcs: (親の要素 ID, 子の要素 ID, weight)。"""
    ids = dict.fromkeys(i for arc in arcs for i in arc[:2])
    body = "".join(loc(f"loc_{i}", i) for i in ids)
    for n, (parent, child, weight) in enumerate(arcs, start=1):
        body += (
            '<link:calculationArc xlink:type="arc" '
            'xlink:arcrole="http://www.xbrl.org/2003/arcrole/summation-item" '
            f'xlink:from="loc_{parent}" xlink:to="loc_{child}" order="{n}" weight="{weight}"/>'
        )
    return f'{HEAD}<link:calculationLink xlink:type="extended" xlink:role="{role}">{body}</link:calculationLink>{TAIL}'.encode()


def labels(items: list[tuple[str, str, str, str]], xsd: str = XSD) -> bytes:
    """items: (要素 ID, ラベルの種類の URI, 言語, 文字列)。"""
    body = ""
    for n, (element_id, role, lang, text) in enumerate(items):
        body += loc(f"loc{n}", element_id, xsd)
        body += f'<link:label xlink:type="resource" xlink:label="label{n}" xlink:role="{role}" xml:lang="{lang}">{text}</link:label>'
        body += (
            '<link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" '
            f'xlink:from="loc{n}" xlink:to="label{n}"/>'
        )
    return f'{HEAD}<link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">{body}</link:labelLink>{TAIL}'.encode()


def schema(imports: dict[str, str]) -> bytes:
    """imports: {モジュール: 版の日付}。"""
    body = "".join(
        f'<xsd:import namespace="http://disclosure.edinet-fsa.go.jp/taxonomy/{m}/{v}/{m}_cor" '
        f'schemaLocation="http://disclosure.edinet-fsa.go.jp/taxonomy/{m}/{v}/{m}_cor_{v}.xsd"/>'
        for m, v in imports.items()
    )
    body += '<xsd:import namespace="http://www.xbrl.org/2003/instance" schemaLocation="http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd"/>'
    return f'<?xml version="1.0"?><xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">{body}</xsd:schema>'.encode()


LABEL = "http://www.xbrl.org/2003/role/label"
VERBOSE = "http://www.xbrl.org/2003/role/verboseLabel"
