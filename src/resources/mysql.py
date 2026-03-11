from dagster import ConfigurableResource
from sqlalchemy import create_engine


class MySQLResource(ConfigurableResource):
    host: str
    port: int
    database: str
    user: str
    password: str

    def get_engine(self):
        return create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            pool_pre_ping=True,
            connect_args={"local_infile": True}
        )