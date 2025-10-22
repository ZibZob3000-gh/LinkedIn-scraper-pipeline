import yaml
import json
import os
from importlib import import_module
from datetime import date
from ingestor.postgres_ingestor import PostgresIngestor
from insertor.postgres_insertor import PostgresInsertor
from extractor.job_extractor import JobExtractor

# ==== LOAD CONFIG ====
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# ==== DATABASE CONFIG ====
postgres_conf = config["postgres"]
db_conf = {
    "host": postgres_conf["host"],
    "dbname": postgres_conf["dbname"],
    "user": postgres_conf["user"],
    "password": postgres_conf["password"],
    "port": postgres_conf.get("port", 5433),
}

# ==== IMPORT EXTRACTOR MODULE(S) DYNAMICALLY ====
skills_module_path = f"{config['paths']['extractors']}.{config['extractors']['skills_extractor'][:-3]}"
skills_module = import_module(skills_module_path)

# ==== LOAD DATA FILES ====
data_dir = config["paths"]["data"]
skills_data = {
    "taxonomy": json.load(open(os.path.join(data_dir, config["skills"]["taxonomy_file"]), "r", encoding="utf-8")),
    "synonyms": json.load(open(os.path.join(data_dir, config["skills"]["synonyms_file"]), "r", encoding="utf-8")),
    "fuzzy_threshold": config["skills"].get("fuzzy_threshold", 85),
}

# ==== LOAD INDUSTRY MAPPINGS ====
industry_mapping_path = os.path.join(data_dir, "industry_mappings.json")
with open(industry_mapping_path, "r", encoding="utf-8") as f:
    industry_mapping = json.load(f)

# ==== LOAD PROMPTS ====
prompts_dir = config["paths"]["prompts"]
prompts = {}
for key, filename in config["prompts"].items():
    with open(os.path.join(prompts_dir, filename), "r", encoding="utf-8") as f:
        prompts[key] = f.read()

# ==== INIT COMPONENTS ====
ingestor = PostgresIngestor(**db_conf)
insertor = PostgresInsertor(**db_conf)
extractor = JobExtractor(
    skills_module=skills_module,
    skills_data=skills_data,
    llm_config=config["llm"],
    prompts=prompts,
    industry_mapping=industry_mapping  # pass the loaded mapping here
)

# ==== FETCH JOBS ====
columns_to_fetch = None  # fetch all columns
date_filter = config.get("processing", {}).get("date_filter")
if date_filter is None:
    date_filter = date.today().isoformat()  # default to today

jobs = ingestor.fetch_job_data(columns=columns_to_fetch, date_filter=date_filter)

# ==== PROCESS AND INSERT ====
max_jobs = config.get("processing", {}).get("max_jobs")  # None if not set
for idx, job in enumerate(jobs):
    if max_jobs is not None and idx >= max_jobs:
        break

    enriched_job = extractor.process_job(job)
    insertor.insert_job(enriched_job)
    print(f"✅ Job {enriched_job['id']} inserted")

print("🎯 All jobs processed and inserted into job_postings_enriched!")
