import json
import requests
import time
import re
import ast
import os
import yaml

# ==== LOAD CONFIG ====
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# ==== LOAD INDUSTRY MAPPINGS ====
data_dir = config["paths"]["data"]
industry_mapping_path = os.path.join(data_dir, "industry_mappings.json")

with open(industry_mapping_path, "r", encoding="utf-8") as f:
    industry_mapping = json.load(f)


# ==== LOCAL LLM CALL ====
def call_local_llm(prompt: str, llm_config: dict) -> str:
    """Send a prompt to a local LLM and return the generated text."""
    model_name = llm_config.get("model_name", "mistral:instruct")
    max_retries = llm_config.get("max_retries", 2)

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


# ==== MAP INDUSTRY ====
def safe_json_parse_industry(text: str) -> dict:
    match = re.search(r'{.*}', text, re.DOTALL)
    if not match:
        return {"main_industry": "unknown", "subindustry": "unknown"}

    json_str = match.group().strip()
    if not json_str.endswith("}"):
        json_str += "}"

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        repaired = re.sub(r',(\s*[}\]])', r'\1', json_str)
        try:
            return json.loads(repaired)
        except Exception:
            return {"main_industry": "unknown", "subindustry": "unknown"}


def map_industry(company_industry_str: str, llm_config: dict) -> dict:
    if not company_industry_str.strip():
        print("⚠️ No industry string provided.")
        return {"main_industry": "unknown", "subindustry": "unknown"}

    # Local keyword fallback
    for main, subs in industry_mapping.items():
        if main.lower() in company_industry_str.lower():
            return {"main_industry": main, "subindustry": "unknown"}
        for sub in subs:
            if sub.lower() in company_industry_str.lower():
                return {"main_industry": main, "subindustry": sub}

    prompt = f"""
You are an expert business analyst.

Return valid JSON in this exact format only:
{{
  "main_industry": "<main_industry>",
  "subindustry": "<subindustry>"
}}

Main industries and subindustries:
{json.dumps(industry_mapping, indent=2)}

Company industry string: "{company_industry_str}"

Rules:
1. Match to the best fitting main_industry and subindustry.
2. If you cannot match, use "unknown" for both.
3. Respond with JSON only — no text, no explanations.
"""
    output_text = call_local_llm(prompt, llm_config)
    print(f"🔍 LLM raw output for industry mapping:\n{output_text}\n")

    return safe_json_parse_industry(output_text)

