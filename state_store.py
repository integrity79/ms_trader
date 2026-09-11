# state_store.py

import json
import os
import tempfile


def atomic_write_json(file_path: str, payload: dict) -> None:
    directory = os.path.dirname(os.path.abspath(file_path))
    file_descriptor, temporary_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(file_path)}.",
        suffix=".tmp",
        dir=directory,
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, file_path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise