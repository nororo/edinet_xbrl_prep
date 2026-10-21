"""書類の ZIP から、有価証券報告書本体（PublicDoc の asr）のファイルを取り出す。

旧版は ``"lab.xml" in name`` のように名前の一部で探し、最初に見つかったファイルを使っていたため、
監査報告書（AuditDoc）などのファイルを拾うことがあった。ここでは PublicDoc にある本体
（ファイル名に ``asr`` を含む）のファイルだけを、種類ごとに1つ選び、中身をメモリに読む。
訂正報告書も同じ規則（訂正後の全文が asr として入っている）。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from zipfile import BadZipFile, ZipFile

# 種類 → ファイル名の末尾
KINDS = {
    "instance": re.compile(r"\.xbrl$"),
    "schema": re.compile(r"\.xsd$"),
    "presentation": re.compile(r"_pre\.xml$"),
    "calculation": re.compile(r"_cal\.xml$"),
    "definition": re.compile(r"_def\.xml$"),
    "label": re.compile(r"_lab\.xml$"),
    "label_en": re.compile(r"_lab-en\.xml$"),
}
PUBLIC_ASR = re.compile(r"XBRL/PublicDoc/[^/]*asr[^/]*$")


class XbrlArchiveError(Exception):
    """ZIP が開けない、または本体のインスタンス・スキーマがない。"""


@dataclass
class XbrlArchive:
    """ZIP 内の有価証券報告書本体のファイル。"""

    path: Path
    names: dict[str, str] = field(default_factory=dict)  # 種類 → ZIP 内のパス
    contents: dict[str, bytes] = field(default_factory=dict)  # 種類 → 中身

    @classmethod
    def open(cls, path: str | Path) -> XbrlArchive:
        archive = cls(Path(path))
        try:
            with ZipFile(path) as zf:
                candidates = sorted(n for n in zf.namelist() if PUBLIC_ASR.search(n))
                for kind, pattern in KINDS.items():
                    matched = [n for n in candidates if pattern.search(n)]
                    if matched:
                        archive.names[kind] = matched[0]
                        archive.contents[kind] = zf.read(matched[0])
        except (BadZipFile, OSError) as exc:
            raise XbrlArchiveError(f"ZIP を開けない: {path} ({exc})") from exc
        if "instance" not in archive.names or "schema" not in archive.names:
            raise XbrlArchiveError(f"有価証券報告書本体のインスタンスまたはスキーマがない: {path}")
        return archive

    def get(self, kind: str) -> bytes | None:
        return self.contents.get(kind)

    def extract(self, target_dir: Path) -> Path:
        """Arelle に読ませるため、本体のファイルを展開し、インスタンスのパスを返す。"""
        out = Path(target_dir) / "XBRL" / "PublicDoc"
        out.mkdir(parents=True, exist_ok=True)
        for kind, data in self.contents.items():
            (out / Path(self.names[kind]).name).write_bytes(data)
        return out / Path(self.names["instance"]).name
