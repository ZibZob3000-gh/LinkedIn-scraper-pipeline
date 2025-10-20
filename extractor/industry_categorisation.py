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
def map_industry(company_industry_str: str, llm_config: dict) -> dict:
    """Map a company industry string to main and subindustry using the LLM."""
    prompt = f"""
You are given the following main industries and their subindustries:
{json.dumps(industry_mapping, indent=2)}

A company has the industry string: "{company_industry_str}"

Return the best matching main industry and subindustry as JSON only, e.g.:
{{"main_industry": "<main_industry>", "subindustry": "<subindustry>"}}
If you cannot match, return null for both fields.
"""
    output_text = call_local_llm(prompt, llm_config)
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
