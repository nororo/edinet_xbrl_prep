"""Extract the files of the annual securities report body (``asr`` in PublicDoc) from a document ZIP."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from zipfile import BadZipFile, ZipFile

# file kind -> file name suffix
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
    """The ZIP cannot be opened, or it has no instance or schema of the report body."""


@dataclass
class XbrlArchive:
    """Files of the annual securities report body in a ZIP."""

    path: Path
    names: dict[str, str] = field(default_factory=dict)  # kind -> path in the ZIP
    contents: dict[str, bytes] = field(default_factory=dict)  # kind -> content

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
        """Write the body files for Arelle to read and return the path of the instance."""
        out = Path(target_dir) / "XBRL" / "PublicDoc"
        out.mkdir(parents=True, exist_ok=True)
        for kind, data in self.contents.items():
            (out / Path(self.names[kind]).name).write_bytes(data)
        return out / Path(self.names["instance"]).name
