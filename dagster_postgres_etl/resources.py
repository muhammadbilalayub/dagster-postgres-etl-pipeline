import os
import psycopg2
import dagster as dg

from dotenv import load_dotenv

load_dotenv()


class PostgresResource(dg.ConfigurableResource):

    def get_connection(self):

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="dagster_db",
            user="dagster",
            password="dagster"
        )

        return conn