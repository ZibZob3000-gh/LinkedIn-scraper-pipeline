import psycopg2
from psycopg2 import sql

# === CONNECTION SETTINGS ===
DB_HOST = "10.0.1.111"
DB_NAME = "exposed_data"
DB_USER = "job_rw_user"
DB_PASSWORD = "qojxy8-cyqxyb-Qebjuw"
DB_PORT = 5433 

# === TABLE CREATION ===
TABLE_NAME = "job_postings_enriched"

create_table_query = sql.SQL("""
CREATE TABLE IF NOT EXISTS public.{table_name} (
    id TEXT PRIMARY KEY,
    title TEXT,
    job_function TEXT,
    job_level TEXT,
    job_type TEXT,
    is_remote BOOLEAN,
    location TEXT,
    date_posted DATE,
    job_url TEXT,
    site TEXT,
    company TEXT,
    company_url TEXT,
    company_industry TEXT,
    description TEXT,
    hard_skills JSONB,
    spoken_languages JSONB,
    contact_details JSONB,
    language_description TEXT                         
);
""").format(table_name=sql.Identifier(TABLE_NAME))

# === EXECUTE CREATION ===
try:
    print("Connecting to database...")
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(create_table_query)
        print(f"✅ Table '{TABLE_NAME}' created successfully in schema 'public' of database '{DB_NAME}'.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals():
        conn.close()
        print("🔒 Connection closed.")
