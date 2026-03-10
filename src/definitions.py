from dagster import Definitions
from src.assets.ingest import kaggle_raw_ev_charging_dataset

defs = Definitions(
    assets=[kaggle_raw_ev_charging_dataset],
)