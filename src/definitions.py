from dagster import Definitions
from src.assets.ingest import kaggle_raw_ev_charging_dataset
from src.assets.bronze import ev_data_landing
from src.resources.mysql import MySQLResource
import os

assets = [kaggle_raw_ev_charging_dataset, ev_data_landing]

resources = {
        "mysql": MySQLResource(
            host=os.getenv("MYSQL_HOST", "mysql"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DB", "ev_platform"),
            user=os.getenv("MYSQL_USER", "ev_user"),
            password=os.getenv("MYSQL_PASSWORD", "ev_pass"),
        )
    }

defs = Definitions(
    assets=assets,
    resources=resources,
)