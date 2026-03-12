import dagster as dg
from sqlalchemy import text

from ..resources.mysql import MySQLResource


@dg.asset(
    deps=["ev_charging_data_landing"]
)
def dim_city_zones(context, mysql: MySQLResource):
    engine = mysql.get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ev_platform.dim_city_zones (
                id SMALLINT UNSIGNED auto_increment NOT NULL,
                city_zone varchar(32) NOT NULL,
                last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT dim_city_zones_pk PRIMARY KEY (id),
                CONSTRAINT dim_city_zones_unique UNIQUE KEY (city_zone)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO dim_city_zones (city_zone)
            SELECT DISTINCT city_zone FROM ev_charging_data_landing;
        """
        ))
        
        
        context.log.info(f"Inserted {result.rowcount} new city zones into dim_city_zones")
        return dg.MaterializeResult(
            metadata={"new_city_zones": result.rowcount},
        )
        
        
@dg.asset(
    deps=["ev_charging_data_landing"]
)
def dim_station_types(context, mysql: MySQLResource):
    engine = mysql.get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ev_platform.dim_station_types (
                id SMALLINT UNSIGNED auto_increment NOT NULL,
                station_type varchar(32) NOT NULL,
                last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT dim_station_types_pk PRIMARY KEY (id),
                CONSTRAINT dim_station_types_unique UNIQUE KEY (station_type)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO dim_station_types (station_type)
            SELECT DISTINCT station_type FROM ev_charging_data_landing;
        """
        ))
        
        
        context.log.info(f"Inserted {result.rowcount} new station types into dim_station_types")
        return dg.MaterializeResult(
            metadata={"new_station_types": result.rowcount},
        )
        
        
@dg.asset(
    deps=["ev_charging_data_landing"]
)
def dim_peak_load_risk_levels(context, mysql: MySQLResource):
    engine = mysql.get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ev_platform.dim_peak_load_risk_levels (
                id SMALLINT UNSIGNED auto_increment NOT NULL,
                peak_load_risk varchar(15) NOT NULL,
                risk_level smallint unsigned not null default 0,
                last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT dim_peak_load_risk_levels PRIMARY KEY (id),
                CONSTRAINT dim_peak_load_risk_levels UNIQUE KEY (peak_load_risk)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO dim_peak_load_risk_levels (peak_load_risk)
            SELECT DISTINCT peak_load_risk FROM ev_charging_data_landing;
        """
        ))
        
        
        context.log.info(f"Inserted {result.rowcount} new peak load risks into dim_peak_load_risk_levels")
        return dg.MaterializeResult(
            metadata={"new_peak_load_risks_levels": result.rowcount},
        )
        
        
@dg.asset(
    deps=[
        "ev_charging_data_landing",
        "dim_city_zones",
        "dim_station_types",
        "dim_peak_load_risk_levels"
    ]
)
def fact_charging_data(context, mysql: MySQLResource):
    engine = mysql.get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ev_platform.fact_charging_data (
                id BIGINT UNSIGNED auto_increment NOT NULL,
                station_type_id SMALLINT UNSIGNED NOT NULL,
                city_zone_id SMALLINT UNSIGNED NOT NULL,
                src_ingest_datetime DATETIME NOT NULL,
                vehicles_charged SMALLINT UNSIGNED NOT NULL,
                avg_charging_duration_minutes DECIMAL(10, 2) NOT NULL,
                energy_dispensed_kwh DECIMAL(10, 2) NOT NULL,
                grid_load_mw DECIMAL(10, 2) NOT NULL,
                renewable_energy_used_percent DECIMAL(5, 2) NOT NULL,
                peak_load_risk_level_id SMALLINT UNSIGNED NOT NULL,
                ingest_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fact_charging_data_pk PRIMARY KEY (id),
                CONSTRAINT fact_charging_data_unique UNIQUE KEY (station_type_id,city_zone_id,ingest_datetime),
                CONSTRAINT fact_charging_data_city_zones_dim_FK FOREIGN KEY (city_zone_id) REFERENCES ev_platform.dim_city_zones(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
                CONSTRAINT fact_charging_data_station_types_dim_FK FOREIGN KEY (station_type_id) REFERENCES ev_platform.dim_station_types(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
                CONSTRAINT fact_charging_data_peak_load_risk_levels_dim_FK FOREIGN KEY (peak_load_risk_level_id) REFERENCES ev_platform.dim_peak_load_risk_levels(id) ON DELETE RESTRICT ON UPDATE RESTRICT
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO fact_charging_data (
                station_type_id,
                city_zone_id,
                ingest_datetime,
                vehicles_charged,
                avg_charging_duration_minutes,
                energy_dispensed_kwh,
                grid_load_mw,
                renewable_energy_used_percent,
                peak_load_risk_level_id
            )
            SELECT
                st.id AS station_type_id,
                cz.id AS city_zone_id,
                l.ingest_datetime,
                l.vehicles_charged,
                l.avg_charging_duration_minutes,
                l.energy_dispensed_kwh,
                l.grid_load_mw,
                l.renewable_energy_used_percent,
                pr.id AS peak_load_risk_level_id
            FROM ev_charging_data_landing l
            INNER JOIN dim_station_types st
                ON st.station_type = l.station_type
            INNER JOIN dim_city_zones cz
                ON cz.city_zone = l.city_zone
            INNER JOIN dim_peak_load_risk_levels pr
                ON pr.peak_load_risk = l.peak_load_risk;
        """))
        
        
        context.log.info(f"Inserted {result.rowcount} new fact rows into fact_charging_data")
        return dg.MaterializeResult(
            metadata={"new_fact_rows": result.rowcount},
        )