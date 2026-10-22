"""
Shared fixtures. The end-to-end fixture runs the real pipeline on 45 simulated
days in a temporary folder, so tests never touch your local data/ or lake/.
"""
import os

import pytest

from shopsense.transform.spark import get_spark

N_DAYS = 45   # backtest needs 28 days of history + 7 days ahead


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "4")
    session = get_spark("shopsense-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def pipeline(tmp_path_factory, spark):
    """Run generate -> ingest -> clean -> curate -> load once; return the paths."""
    from shopsense.generate.retail_data import generate
    from shopsense.ingest.land_raw import ingest
    from shopsense.transform import clean, curate
    from shopsense.warehouse.load import load

    root = tmp_path_factory.mktemp("shopsense")
    landing, lake, db = root / "landing", root / "lake", root / "warehouse.duckdb"
    counts = generate(n_days=N_DAYS, seed=7, landing=landing)
    ingest(landing=landing, lake=lake)
    clean_stats = clean.run(spark, lake).tables
    curated = curate.run(spark, lake)
    loaded = load(lake=lake, db_path=db)
    return {"root": root, "landing": landing, "lake": lake, "db": db, "generated": counts,
            "clean": clean_stats, "curated": curated, "loaded": loaded}
