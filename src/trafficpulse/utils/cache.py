from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CacheKey:
    namespace: str
    key: str

    def hashed(self) -> str:
        return _sha256(f"{self.namespace}:{self.key}")


class FileCache:
    def __init__(self, directory: Path, ttl_seconds: int = 3600, enabled: bool = True):
        self.directory = directory
        self.ttl_seconds = ttl_seconds
        self.enabled = enabled
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path_for(self, cache_key: CacheKey, ext: str) -> Path:
        safe_namespace = cache_key.namespace.replace("/", "_")
        digest = cache_key.hashed()
        subdir = self.directory / safe_namespace / digest[:2]
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir / f"{digest}{ext}"

    def _is_expired(self, path: Path) -> bool:
        if self.ttl_seconds <= 0:
            return False
        try:
            age_seconds = time.time() - path.stat().st_mtime
        except FileNotFoundError:
            return True
        return age_seconds > self.ttl_seconds

    def get_text(self, namespace: str, key: str) -> Optional[str]:
        if not self.enabled:
            return None
        path = self._path_for(CacheKey(namespace, key), ".txt")
        if not path.exists() or self._is_expired(path):
            path.unlink(missing_ok=True)
            return None
        return path.read_text(encoding="utf-8")

    def set_text(self, namespace: str, key: str, value: str) -> Path:
        if not self.enabled:
            return self._path_for(CacheKey(namespace, key), ".txt")
        path = self._path_for(CacheKey(namespace, key), ".txt")
        tmp = path.with_suffix(f"{path.suffix}.tmp")
        tmp.write_text(value, encoding="utf-8")
        tmp.replace(path)
        return path

    def get_json(self, namespace: str, key: str) -> Optional[Any]:
        if not self.enabled:
            return None
        path = self._path_for(CacheKey(namespace, key), ".json")
        if not path.exists() or self._is_expired(path):
            path.unlink(missing_ok=True)
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def set_json(self, namespace: str, key: str, value: Any) -> Path:
        if not self.enabled:
            return self._path_for(CacheKey(namespace, key), ".json")
        path = self._path_for(CacheKey(namespace, key), ".json")
        tmp = path.with_suffix(f"{path.suffix}.tmp")
        tmp.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
        return path

    def clear_namespace(self, namespace: str) -> int:
        if not self.enabled:
            return 0
        safe_namespace = namespace.replace("/", "_")
        root = self.directory / safe_namespace
        if not root.exists():
            return 0
        count = 0
        for path in root.rglob("*"):
            if path.is_file():
                path.unlink(missing_ok=True)
                count += 1
        return count

