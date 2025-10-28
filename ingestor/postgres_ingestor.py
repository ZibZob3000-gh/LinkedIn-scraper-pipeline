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

    def fetch_job_data(self,
                       table_name="job_postings", columns: list[str] | None = None,
                       date_filter: str | None = None,
                       date_range: tuple[str, str] | None = None
                       ):
        """
        Fetch job postings, optionally filtering by ingested_at date or range.

        Args:
            table_name (str): name of the table to query.
            columns (list): list of column names to fetch; defaults to all.
            date_filter (str): specific date in 'YYYY-MM-DD' format.
            date_range (tuple): (start_date, end_date) in 'YYYY-MM-DD' format.
        """
        # ========================
        # Date selection priority
        # ========================
        if date_filter:
            where_clause = "WHERE ingested_at::date = %s"
            params = (date_filter,)
            mode = f"single date ({date_filter})"

        elif date_range and len(date_range) == 2:
            start_date, end_date = date_range
            where_clause = "WHERE ingested_at::date BETWEEN %s AND %s"
            params = (start_date, end_date)

        else:
            today = date.today().isoformat()
            where_clause = "WHERE ingested_at::date = %s"
            params = (today,)
            mode = f"defaulted to today ({today})"

        # ========================
        # Build and execute query
        # ========================
        select_clause = ", ".join(columns) if columns else "*"
        query = f"""
        SELECT {select_clause}
        FROM {table_name}
        {where_clause}
        ORDER BY id ASC;
        """

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

                # Get column names dynamically if columns are not provided
                if columns is None:
                    columns = [desc[0] for desc in cur.description]

        # Convert each row to a dictionary
        return [dict(zip(columns, row)) for row in rows]