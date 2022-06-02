from urllib.parse import urlparse

import psycopg2

from src.trade_transform.postgres_config import get_postgres_url, get_postgres_user, get_postgres_password
from src.utils.logger_builder import LoggerBuilder

logger = LoggerBuilder().build()


class Database:
    """PostgreSQL Database class."""

    def __init__(self, env: str):
        self.env = env
        self.conn = None
        self.rows_affected = 0

    def connect(self):
        logger.info("Connect to a Postgres database.")
        if self.conn is None:
            try:
                domain = urlparse(get_postgres_url(self.env)).path
                self.conn = psycopg2.connect(database=domain.split("/")[3],
                                             user=get_postgres_user(self.env),
                                             password=get_postgres_password(self.env),
                                             host=domain.split("/")[2].split(":")[0],
                                             port=domain.split("/")[2].split(":")[1])
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(error)
                raise Exception
            finally:
                logger.info('Postgres Connection opened successfully.')

    def execute_query(self, query, values):
        logger.info(f"Run a SQL query in Postgres table.")
        self.connect()
        with self.conn.cursor() as cur:
            logger.info("Query Execution Started")
            cur.execute(query, values)
            rows_affected = cur.rowcount
            self.conn.commit()
            cur.close()
            return f"{rows_affected} rows affected."
