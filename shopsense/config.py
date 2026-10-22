"""
Central configuration: where the landing area, lake zones and warehouse live.

Every path can be overridden with an environment variable, so the same code
runs against a local folder today and an S3 bucket later (stage 2).

Lake layout (mirrors an S3 bucket):
    landing/                 files exactly as source systems drop them
    lake/raw/<source>/ingest_date=YYYY-MM-DD/   immutable copy of each delivery
    lake/clean/<table>/      typed, deduplicated Parquet, partitioned by business date
    lake/curated/<table>/    star schema (dimensions + facts) as Parquet
    lake/quarantine/<table>/ rows rejected by cleaning rules, with a reason
"""
import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Paths:
    landing: Path
    lake: Path
    warehouse_db: Path

    @property
    def raw(self) -> Path:
        return self.lake / "raw"

    @property
    def clean(self) -> Path:
        return self.lake / "clean"

    @property
    def curated(self) -> Path:
        return self.lake / "curated"

    @property
    def quarantine(self) -> Path:
        return self.lake / "quarantine"


def get_paths() -> Paths:
    return Paths(
        landing=Path(os.getenv("SHOPSENSE_LANDING", ROOT / "data" / "landing")),
        lake=Path(os.getenv("SHOPSENSE_LAKE", ROOT / "lake")),
        warehouse_db=Path(os.getenv("SHOPSENSE_WAREHOUSE", ROOT / "warehouse" / "shopsense.duckdb")),
    )


# Sources delivered by upstream systems, and the format each arrives in.
SOURCES = {
    "orders": "csv",
    "order_items": "csv",
    "order_status_updates": "csv",
    "products": "csv",
    "customers": "csv",
    "inventory_snapshots": "csv",
    "promotions": "csv",
    "clickstream": "json.gz",
    "reviews": "json",
}
