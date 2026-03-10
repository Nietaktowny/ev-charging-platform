from dagster import Definitions
from src.assets.placeholder_asset_to_start import hello_asset

defs = Definitions(
    assets=[hello_asset],
)