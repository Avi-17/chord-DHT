import json
import os
from pathlib import Path
from typing import List, Union

class FileManager:
    def __init__(self, base_directory: str):
        self.base_path = Path(base_directory)
        self._ensure_directory_exists(self.base_path)

    def _ensure_directory_exists(self, path: Path):
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

    def _get_full_path(self, filename: str) -> Path:
        return self.base_path / filename

    # --- Binary Operations (For Encrypted Data) ---
    def write_bytes(self, filename: str, data: bytes) -> bool:
        path = self._get_full_path(filename)
        try:
            with open(path, "wb") as f:
                f.write(data)
            return True
        except IOError as e:
            print(f"[FileManager] Error writing bytes: {e}")
            return False

    def read_bytes(self, filename: str) -> bytes:
        path = self._get_full_path(filename)
        if not path.exists():
            return None
        with open(path, "rb") as f:
            return f.read()

    # --- JSON Operations (For Metadata) ---
    def write_json(self, filename: str, data: dict) -> bool:
        path = self._get_full_path(filename)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            return True
        except IOError:
            return False

    def read_json(self, filename: str) -> dict:
        path = self._get_full_path(filename)
        if not path.exists():
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}

    # --- Utility ---
    def list_files(self) -> List[str]:
        """Lists filenames excluding .meta files."""
        return [f.name for f in self.base_path.glob("*") if f.is_file() and not f.name.endswith('.meta')]