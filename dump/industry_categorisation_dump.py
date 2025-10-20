import json
import requests
import time
import re
import ast

# ==== CONFIG ====
INPUT_FILE = "Industries.json"  # your input file [{"company_industry": "..."}]
INDUSTRY_MAPPING_FILE = "industry_mappings.json"  # main/subindustry mapping
MODEL_NAME = "mistral:instruct"
MAX_RETRIES = 2
MAX_JOBS = 2  # process only the first N jobs (set None to process all)

# ==== LOAD INPUT DATA ====
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    jobs = json.load(f)

with open(INDUSTRY_MAPPING_FILE, "r", encoding="utf-8") as f:
    industry_mapping = json.load(f)


# ==== LOCAL LLM CALL ====
def call_local_llm(prompt: str, model_name: str = MODEL_NAME, max_retries: int = MAX_RETRIES) -> str:
    """Call your local LLM API and return raw text."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": model_name, "prompt": prompt},
                stream=True
            )
            full_output = ""
            for chunk in response.iter_lines():
                if chunk:
                    decoded = chunk.decode("utf-8")
                    try:
                        json_chunk = json.loads(decoded)
                        full_output += json_chunk.get("response", "")
                    except json.JSONDecodeError:
                        full_output += decoded
            return full_output
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            time.sleep(1)
    return ""


# ==== MAP INDUSTRY USING LLM ====
def map_industry(company_industry_str: str) -> dict:
    """
    Map a company_industry string (possibly multiple comma-separated industries)
    to main_industry and subindustry using the local LLM.
    """
    prompt = f"""
You are given the following main industries and their subindustries:
{json.dumps(industry_mapping, indent=2)}

A company has the industry string: "{company_industry_str}"

Return the best matching main industry and subindustry as JSON only, e.g.:
{{"main_industry": "<main_industry>", "subindustry": "<subindustry>"}}
If you cannot match, return null for both fields.
"""
    output_text = call_local_llm(prompt)

    # Attempt to parse JSON from LLM response
    try:
        match = re.search(r'{.*}', output_text, re.DOTALL)
        if match:
            json_str = match.group()
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                return ast.literal_eval(json_str)
    except Exception:
        pass
    return {"main_industry": None, "subindustry": None}


# ==== PROCESS AND PRINT JOBS ====
for idx, job in enumerate(jobs):
    if MAX_JOBS is not None and idx >= MAX_JOBS:
        break

    company_industry = job.get("company_industry", "")
    mapping = map_industry(company_industry)
    print(f"Original Industry: {company_industry}")
    print(f"  -> Main Industry: {mapping.get('main_industry')}")
    print(f"  -> Subindustry: {mapping.get('subindustry')}")
    print("-" * 60)

print("✅ Done mapping all industries!")
