import json
from pathlib import Path
from typing import Dict


class UsageTracker:
    def __init__(
        self,
        key: str,
        limit: int,
        storage_path: Path | str = Path("usage.json"),
    ) -> None:
        self.key = key
        self.limit = limit
        self.storage_path = Path(storage_path)
        self._data = self._load()

    def _load(self) -> Dict[str, int]:
        if self.storage_path.exists():
            try:
                return json.loads(self.storage_path.read_text())
            except json.JSONDecodeError:
                pass
        return {self.key: self.limit}

    def _save(self) -> None:
        self.storage_path.write_text(json.dumps(self._data, indent=2))

    def remaining(self) -> int:
        return self._data.get(self.key, self.limit)

    def can_consume(self) -> bool:
        return self.remaining() > 0

    def consume(self) -> bool:
        if not self.can_consume():
            return False
        self._data[self.key] = self.remaining() - 1
        self._save()
        return True

    def reset(self) -> None:
        self._data[self.key] = self.limit
        self._save()
