from pathlib import Path
import shutil
from src.config import path_for

SOURCE_FILES = ["customers.csv", "products.json", "orders.csv"]


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    Creates data/raw/run_id=<run_id>/ and copies each source file into it
    byte-for-byte, without altering content. This gives every pipeline run
    a reproducible, timestamped input snapshot independent of whatever the
    source files look like later.
    """
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f"run_id={run_id}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for filename in SOURCE_FILES:
        src_path = source_dir / filename
        if not src_path.exists():
            raise FileNotFoundError(
                f"Expected source file missing: {src_path}. "
                f"Extraction stage cannot proceed without all source files."
            )
        shutil.copy2(src_path, raw_dir / filename)

    return raw_dir