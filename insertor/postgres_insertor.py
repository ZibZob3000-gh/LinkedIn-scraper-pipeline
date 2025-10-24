# insertor/postgres_insertor.py
import json

class PostgresInsertor:
    def __init__(self, host=None, dbname=None, user=None, password=None, port=5433):
        """
        Print-only version: does not connect to a database.
        Keeps same interface so you can reuse it in your pipeline.
        """
        self.conn_params = {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port
        }

    def insert_job(self, job_data, table_name="job_postings_enriched"):
        """
        Simulates an insert into PostgreSQL by printing a table
        with headers and values, instead of performing a DB operation.
        """

        all_columns = list(job_data.keys())
        values = []

        for col in all_columns:
            val = job_data[col]
            # Convert dicts or lists to JSON strings
            if isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)
            values.append(val)

        # Print simulated query
        print(f"\n🧩 Simulated insert into table: '{table_name}'")
        print("Would execute an UPSERT (insert or update on conflict)\n")

        # Prepare column headers and values for printing
        col_widths = [max(len(str(c)), len(str(v))) for c, v in zip(all_columns, values)]
        total_width = sum(col_widths) + 3 * len(all_columns) + 1

        # Print header
        print("+" + "-" * (total_width - 2) + "+")
        header_row = "| " + " | ".join(f"{c:<{col_widths[i]}}" for i, c in enumerate(all_columns)) + " |"
        print(header_row)
        print("+" + "-" * (total_width - 2) + "+")

        # Print row
        value_row = "| " + " | ".join(f"{str(v):<{col_widths[i]}}" for i, v in enumerate(values)) + " |"
        print(value_row)
        print("+" + "-" * (total_width - 2) + "+")

        # Optional debug info
        print("\n✅ This data would be inserted/updated in the database if connected.\n")
