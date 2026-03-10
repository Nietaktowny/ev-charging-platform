from dagster import asset
import pandas as pd
from pathlib import Path


@asset
def hello_asset() -> str:
    path = Path("/opt/dagster/app/data/raw")
    path.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{"message": "Dagster + MySQL + Postgres works"}])
    df.to_csv(path / "hello.csv", index=False)
    return "ok"