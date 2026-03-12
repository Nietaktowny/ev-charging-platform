import dagster as dg
from sqlalchemy import text
from pathlib import Path

from ..resources.mysql import MySQLResource


@dg.asset(
    ins={"kaggle_raw_ev_charging_dataset": dg.AssetIn()}
)
def ev_charging_data_landing(context, mysql: MySQLResource, kaggle_raw_ev_charging_dataset: str):
    engine = mysql.get_engine()
    with engine.begin() as conn:
        # Create landing table if it doesn't exist
        context.log.debug("Ensuring landing table exists")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ev_platform.ev_charging_data_landing (
                id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,
                ingest_datetime DATETIME NOT NULL,
                city_zone VARCHAR(32) NOT NULL,
                station_type VARCHAR(32) NOT NULL,
                vehicles_charged SMALLINT UNSIGNED NOT NULL,
                avg_charging_duration_minutes DECIMAL(10, 2) NOT NULL,
                energy_dispensed_kwh DECIMAL(10, 2) NOT NULL,
                grid_load_mw DECIMAL(10, 2) NOT NULL,
                renewable_energy_used_percent DECIMAL(5, 2) NOT NULL,
                peak_load_risk VARCHAR(15) NOT NULL,
                CONSTRAINT ev_charging_data_landing_pk PRIMARY KEY (id),
                INDEX ev_charging_data_landing_station_type_IDX (station_type),
                INDEX ev_charging_data_landing_peak_load_risk_IDX (peak_load_risk),
                INDEX ev_charging_data_landing_city_zone_IDX (city_zone),
                UNIQUE INDEX ev_charging_data_landing_ingest_datetime_IDX (ingest_datetime,city_zone,station_type)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
        """))
        
        
        file_path = Path(kaggle_raw_ev_charging_dataset).resolve().as_posix()

        sql = text(f"""
            LOAD DATA LOCAL INFILE '{file_path}'
            INTO TABLE ev_charging_data_landing
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
                @record_id,
                ingest_datetime,
                city_zone,
                station_type,
                vehicles_charged,
                avg_charging_duration_minutes,
                energy_dispensed_kwh,
                grid_load_mw,
                renewable_energy_used_percent,
                peak_load_risk
            )
        """)

        result = conn.execute(sql)
        context.log.info(f"Loaded {result.rowcount} rows into ev_charging_data_landing from {file_path}")
        return dg.MaterializeResult(
            metadata={"rows_loaded": result.rowcount},
        )