# ingestor.py
import psycopg2
from datetime import date

class PostgresIngestor:
    def __init__(self, host, dbname, user, password, port=5432):
        self.conn_params = {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port
        }

    def fetch_job_data(self, table_name="job_postings", columns=None, date_filter=None):
        """
        Fetch job postings, optionally filtering by ingested_at date.
        date_filter: str in 'YYYY-MM-DD' format or None for today
        """
        if date_filter is None:
            date_filter = date.today().isoformat()  # default to today

        # Prepare query
        select_clause = ", ".join(columns) if columns else "*"
        query = f"""
        SELECT {select_clause}
        FROM {table_name}
        WHERE ingested_at::date = %s
        ORDER BY id ASC;
        """

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (date_filter,))
                rows = cur.fetchall()

                # Get column names dynamically if columns is None
                if columns is None:
                    columns = [desc[0] for desc in cur.description]

        # Convert each row to a dict
        return [dict(zip(columns, row)) for row in rows]
