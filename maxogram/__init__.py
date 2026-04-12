from __future__ import annotations

from pathlib import Path

_src_pkg = Path(__file__).resolve().parent.parent / "src" / "maxogram"
if _src_pkg.exists():
    __path__.append(str(_src_pkg))

__version__ = "0.1.0"
