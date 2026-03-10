import dagster as dg
from pathlib import Path
import kagglehub


@dg.asset
def kaggle_raw_ev_charging_dataset(context) -> str:
    output_dir = Path("/opt/dagster/app/data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    dataset = "jayjoshi37/ev-charging-station-usage-and-grid-load-analysis"

    context.log.debug(f"Downloading dataset with handle '{dataset}' to: {str(output_dir)}")
    
    path = kagglehub.dataset_download(
        dataset,
        output_dir=str(output_dir),
        force_download=False,
    )

    context.log.info(f"Dataset downloaded to: {path}")
    return dg.MaterializeResult(
        value=str(path)
    )