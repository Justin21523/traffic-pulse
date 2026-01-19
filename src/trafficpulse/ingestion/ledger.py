from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def safe_append_ledger_entry(path: Path, entry: dict[str, Any]) -> None:
    """Append a single JSON line to an ingest ledger file.

    This is best-effort: ingestion should not fail if the ledger cannot be written.
    """

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
        path.open("a", encoding="utf-8").write(payload + "\n")
    except Exception:
        return


def read_latest_ledger_entry(path: Path) -> dict[str, Any] | None:
    """Return the latest valid JSON entry from a JSONL ledger, or None if not available."""

    if not path.exists():
        return None

    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            end = f.tell()
            if end <= 0:
                return None

            chunk_size = 8192
            buffer = b""
            pos = end
            while pos > 0:
                read_size = min(chunk_size, pos)
                pos -= read_size
                f.seek(pos)
                buffer = f.read(read_size) + buffer
                if b"\n" not in buffer:
                    continue
                lines = buffer.splitlines()
                for raw_line in reversed(lines):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        parsed = json.loads(line.decode("utf-8", errors="ignore"))
                    except Exception:
                        continue
                    if isinstance(parsed, dict):
                        return parsed
            return None
    except Exception:
        return None

