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
            CREATE TABLE IF NOT EXISTS ev_platform.city_zones_dim (
                id SMALLINT UNSIGNED auto_increment NOT NULL,
                city_zone varchar(32) NOT NULL,
                CONSTRAINT city_zones_dim_pk PRIMARY KEY (id),
                CONSTRAINT city_zones_dim_unique UNIQUE KEY (city_zone)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO city_zones_dim (city_zone)
            SELECT DISTINCT city_zone FROM ev_charging_data_landing;
        """
        ))
        
        
        context.log.info(f"Inserted {result.rowcount} new city zones into city_zones_dim")
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
            CREATE TABLE IF NOT EXISTS ev_platform.station_types_dim (
                id SMALLINT UNSIGNED auto_increment NOT NULL,
                station_type varchar(32) NOT NULL,
                CONSTRAINT station_types_dim_pk PRIMARY KEY (id),
                CONSTRAINT station_types_dim_unique UNIQUE KEY (station_type)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO station_types_dim (station_type)
            SELECT DISTINCT station_type FROM ev_charging_data_landing;
        """
        ))
        
        
        context.log.info(f"Inserted {result.rowcount} new station types into station_types_dim")
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
            CREATE TABLE IF NOT EXISTS ev_platform.peak_load_risk_levels_dim (
                id SMALLINT UNSIGNED auto_increment NOT NULL,
                peak_load_risk varchar(15) NOT NULL,
                CONSTRAINT peak_load_risk_levels_dim_pk PRIMARY KEY (id),
                CONSTRAINT peak_load_risk_levels_dim_unique UNIQUE KEY (peak_load_risk)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
        """))
        
        result = conn.execute(text("""
            INSERT IGNORE INTO peak_load_risk_levels_dim (peak_load_risk)
            SELECT DISTINCT peak_load_risk FROM ev_charging_data_landing;
        """
        ))
        
        
        context.log.info(f"Inserted {result.rowcount} new peak load risks into peak_load_risk_levels_dim")
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
                ingest_datetime DATETIME NOT NULL,
                vehicles_charged SMALLINT UNSIGNED NOT NULL,
                avg_charging_duration_minutes DECIMAL(10, 2) NOT NULL,
                energy_dispensed_kwh DECIMAL(10, 2) NOT NULL,
                grid_load_mw DECIMAL(10, 2) NOT NULL,
                renewable_energy_used_percent DECIMAL(5, 2) NOT NULL,
                peak_load_risk_level_id SMALLINT UNSIGNED NOT NULL,
                CONSTRAINT fact_charging_data_pk PRIMARY KEY (id),
                CONSTRAINT fact_charging_data_unique UNIQUE KEY (station_type_id,city_zone_id,ingest_datetime),
                CONSTRAINT fact_charging_data_city_zones_dim_FK FOREIGN KEY (city_zone_id) REFERENCES ev_platform.city_zones_dim(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
                CONSTRAINT fact_charging_data_station_types_dim_FK FOREIGN KEY (station_type_id) REFERENCES ev_platform.station_types_dim(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
                CONSTRAINT fact_charging_data_peak_load_risk_levels_dim_FK FOREIGN KEY (peak_load_risk_level_id) REFERENCES ev_platform.peak_load_risk_levels_dim(id) ON DELETE RESTRICT ON UPDATE RESTRICT
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
            INNER JOIN station_types_dim st
                ON st.station_type = l.station_type
            INNER JOIN city_zones_dim cz
                ON cz.city_zone = l.city_zone
            INNER JOIN peak_load_risk_levels_dim pr
                ON pr.peak_load_risk = l.peak_load_risk;
        """))
        
        
        context.log.info(f"Inserted {result.rowcount} new fact rows into fact_charging_data")
        return dg.MaterializeResult(
            metadata={"new_fact_rows": result.rowcount},
        )