# insertor/postgres_insertor.py
import psycopg2
from psycopg2 import sql
import json


class PostgresInsertor:
    def __init__(self, host, dbname, user, password, port=5433):
        self.conn_params = {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port
        }

    def insert_job(self, job_data, table_name="job_postings_enriched"):
        """
        Inserts or updates a job record into the enriched table.
        - Converts dicts/lists to JSONB automatically.
        - Replaces existing columns with same name.
        - Appends new ones if the table supports them.
        """
        all_columns = list(job_data.keys())
        placeholders = [f"%s"] * len(all_columns)

        values = []
        for col in all_columns:
            val = job_data[col]

            # Convert dicts or lists to JSON
            if isinstance(val, (dict, list)):
                values.append(json.dumps(val))
            else:
                values.append(val)

        insert_query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE
            SET {updates};
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, all_columns)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(all_columns)),
            updates=sql.SQL(', ').join([
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in all_columns if col != "id"
            ])
        )

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, values)
