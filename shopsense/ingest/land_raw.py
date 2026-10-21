"""
Ingest files from the landing area into the raw zone of the lake.

    landing/<source>/<YYYY-MM-DD>/<file>  ->  lake/raw/<source>/ingest_date=<YYYY-MM-DD>/<file>

Rules
    * Raw is immutable: files are copied byte-for-byte, never modified.
    * Idempotent: each batch gets a _manifest.json with a SHA-256 per file.
      Re-ingesting an identical batch is skipped; a changed file for an
      already-ingested date is refused unless --force is given, so a silent
      upstream re-delivery can't overwrite history.
    * Auditable: every batch is appended to lake/_audit/ingest_log.jsonl.

Hive-style `ingest_date=` folders let Spark (and later AWS Glue/Athena)
treat the date as a partition column without listing every file.

Usage:
    python -m shopsense.ingest.land_raw                 # everything in landing
    python -m shopsense.ingest.land_raw --date 2026-03-01
"""
import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from shopsense.config import SOURCES, get_paths


class ChecksumMismatch(Exception):
    """An already-ingested batch was re-delivered with different content."""


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_batch(source: str, batch_dir: Path, raw_root: Path, force: bool = False) -> dict:
    """Copy one landing batch (a source + date folder) into raw. Returns an audit record."""
    ingest_date = batch_dir.name
    target = raw_root / source / f"ingest_date={ingest_date}"
    files = sorted(p for p in batch_dir.iterdir() if p.is_file())
    manifest = {"source": source, "ingest_date": ingest_date,
                "files": {p.name: {"sha256": sha256(p), "bytes": p.stat().st_size} for p in files}}

    existing = target / "_manifest.json"
    if existing.exists():
        previous = json.loads(existing.read_text())
        if previous["files"] == manifest["files"]:
            return {**manifest, "status": "skipped_duplicate"}
        if not force:
            raise ChecksumMismatch(f"{source} {ingest_date}: re-delivered with different content (use --force)")
        shutil.rmtree(target)

    target.mkdir(parents=True, exist_ok=True)
    for p in files:
        shutil.copy2(p, target / p.name)
    manifest["ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    existing.write_text(json.dumps(manifest, indent=2))
    return {**manifest, "status": "ingested"}


def ingest(landing: Path | None = None, lake: Path | None = None, date: str | None = None,
           force: bool = False) -> list[dict]:
    paths = get_paths()
    landing = landing or paths.landing
    raw_root = (lake or paths.lake) / "raw"
    audit = (lake or paths.lake) / "_audit" / "ingest_log.jsonl"
    audit.parent.mkdir(parents=True, exist_ok=True)

    results = []
    for source in SOURCES:
        src_dir = landing / source
        if not src_dir.exists():
            continue
        for batch in sorted(d for d in src_dir.iterdir() if d.is_dir()):
            if date and batch.name != date:
                continue
            record = ingest_batch(source, batch, raw_root, force)
            results.append(record)
            with audit.open("a") as fh:
                fh.write(json.dumps({k: v for k, v in record.items() if k != "files"}
                                    | {"n_files": len(record["files"]),
                                       "bytes": sum(f["bytes"] for f in record["files"].values())}) + "\n")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest landing files into the raw zone.")
    parser.add_argument("--date", help="only ingest batches for this date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="replace a re-delivered batch")
    args = parser.parse_args()

    try:
        results = ingest(date=args.date, force=args.force)
    except ChecksumMismatch as exc:
        raise SystemExit(f"ERROR: {exc}")
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    print(f"batches: {len(results):,}  " + "  ".join(f"{k}={v:,}" for k, v in sorted(by_status.items())))
    print(f"raw zone -> {get_paths().raw}")


if __name__ == "__main__":
    main()
