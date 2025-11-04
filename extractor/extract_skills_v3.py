import json
from mistralai import Mistral
import time
import re
import ast
from rapidfuzz import process, fuzz
import random


# ==== TEXT CLEANUP HELPER ====
def clean_job_description(text: str) -> str:
    text = text.replace('\t', ' ').replace('\r\n', '\n').replace('\r', '\n')
    text = text.replace('\\-', '-')

    lines = [line.rstrip() for line in text.splitlines()]

    cleaned_lines = []
    previous_empty = False
    for line in lines:
        if line.strip() == '':
            if not previous_empty:
                cleaned_lines.append('')
            previous_empty = True
        else:
            cleaned_lines.append(line)
            previous_empty = False

    return '\n'.join(cleaned_lines).strip()


# ==== SAFE JSON PARSER (UNIFIED) ====
def safe_json_parse(text: str) -> dict:
    """Try to safely extract and repair JSON from a possibly malformed LLM output."""
    text_clean = re.sub(r'//.*', '', text)
    text_clean = re.sub(r'\(".*?"\)|\s*\(.*?\)', '', text_clean)
    text_clean = re.sub(r'<.*?>', '', text_clean)

    json_match = re.search(r'{.*}', text_clean, re.DOTALL)
    if json_match:
        json_str = json_match.group()
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass

    start_idx = text_clean.find('{')
    if start_idx == -1:
        raise ValueError("❌ No JSON object found in LLM output")

    json_str = text_clean[start_idx:].strip()
    lines = json_str.splitlines()
    json_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith('"') or stripped.endswith(('}', ']', ',', '{', '[')):
            line_clean = re.sub(r'\s*\(.*?\)', '', line)
            line_clean = re.sub(r'//.*', '', line_clean)
            json_lines.append(line_clean)
        else:
            break

    if not json_lines:
        raise ValueError("❌ No JSON-like content found in LLM output")

    repaired_json_str = "\n".join(json_lines)
    if not repaired_json_str.rstrip().endswith('}'):
        repaired_json_str = repaired_json_str.rstrip() + "\n}"

    repaired_json_str = re.sub(r',(\s*[}\]])', r'\1', repaired_json_str)

    try:
        return json.loads(repaired_json_str)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(repaired_json_str)
        except Exception:
            return {}


# ==== SKILL MAPPING ====
def map_skill_with_synonyms_verbose(
    extracted_skill: str,
    synonym_to_skill: dict,
    skill_lookup: dict,
    all_synonyms: list,
    fuzzy_threshold: int = 95
) -> dict:
    s_norm = extracted_skill.strip().lower()

    if s_norm in synonym_to_skill:
        base_skill = synonym_to_skill[s_norm]
        skill_id = skill_lookup.get(base_skill.lower(), {}).get("ID")
        return {"mapped_to": base_skill, "ID": skill_id}

    best = process.extractOne(s_norm, all_synonyms, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= fuzzy_threshold:
        base_skill = synonym_to_skill[best[0].lower()]
        skill_id = skill_lookup.get(base_skill.lower(), {}).get("ID")
        return {"mapped_to": base_skill, "ID": skill_id}

    return {"mapped_to": None, "ID": None}


# ==== LOCAL LLM CALL ====
def call_llm(prompt: str, llm_config: dict) -> str:
    """
    Calls Mistral's hosted API model (via La Plateforme) using a robust
    exponential backoff with jitter for handling 429 / capacity errors.
    All parameters are configurable via llm_config.
    """
    api_key = llm_config.get("api_key")
    model_name = llm_config.get("model_name", "mistral-small-latest")
    max_retries = llm_config.get("max_retries", 5)
    base_delay = llm_config.get("base_delay", 1)     # starting wait in seconds
    max_delay = llm_config.get("max_delay", 30)      # max wait in seconds
    jitter = llm_config.get("jitter", 1)             # random jitter to spread retries

    client = Mistral(api_key=api_key)

    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.complete(
                model=model_name,
                messages=[{"role": "user", "content": prompt}]
            )

            # Check if response exists and has choices
            if response and response.choices:
                return response.choices[0].message.content.strip()

        except Exception as e:
            # Calculate exponential backoff with jitter
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            delay += random.uniform(0, jitter)
            print(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)

    raise RuntimeError(f"❌ Mistral API call failed after {max_retries} attempts. Check your key or network.")


# ==== SKILL + LANGUAGE EXTRACTION (LLM Call 1) ====
def extract_skills(description: str, llm_config: dict, prompts: dict) -> dict:
    prompt_template = prompts.get("prompt_1") or prompts.get("single_prompt", "")
    prompt = prompt_template.replace("{job_description}", description)

    try:
        output_text = call_llm(prompt, llm_config)

        data = safe_json_parse(output_text)
        hard_skills = data.get("hard_skills", [])
        soft_skills = data.get("soft_skills", [])

        return {"hard_skills": hard_skills, "soft_skills": soft_skills}

    except Exception as e:
        print(f"⚠️ extract_skills failed: {e}")
        return {"hard_skills": [], "soft_skills": []}


# ==== LANGUAGE, DEPARTMENT, CONTACT (LLM Call 2) ====
def extract_metadata(description: str, llm_config: dict, prompts: dict) -> dict:
    prompt_template = prompts.get("prompt_2") or prompts.get("single_prompt", "")
    prompt = prompt_template.replace("{job_description}", description)

    try:
        output_text = call_llm(prompt, llm_config)
        data = safe_json_parse(output_text)

        return {
            "spoken_languages": data.get("spoken_languages", []),
            "department": data.get("department", "null"),
            "contact_details": data.get("contact_details", {
                "name": "not provided",
                "email": "not provided",
                "phone_number": "not provided"
            }),
            "language_description": data.get("language_description", "not detected")
        }

    except Exception as e:
        print(f"⚠️ extract_language_and_contact failed: {e}")
        return {
            "spoken_languages": [],
            "department": "null",
            "contact_details": {
                "name": "not provided",
                "email": "not provided",
                "phone_number": "not provided"
            },
            "language_description": "not detected"
        }


# ==== SKILL & LANGUAGE LEVELS (LLM Call 3) ====
def extract_skill_levels(hard_skills: list, spoken_languages: list, description: str, llm_config: dict, prompts: dict) -> dict:
    prompt_template = prompts.get("prompt_3") or prompts.get("single_prompt", "")
    hard_skills_str = ", ".join(hard_skills)
    spoken_languages_str = ", ".join(spoken_languages)
    prompt = prompt_template.replace("{job_description}", description)\
                            .replace("{hard_skills}", hard_skills_str)\
                            .replace("{spoken_languages}", spoken_languages_str)

    try:
        output_text = call_llm(prompt, llm_config)
        data = safe_json_parse(output_text)

        return {
            "hard_skill_levels": data.get("hard_skill_levels", {}),
            "spoken_languages_levels": data.get("spoken_languages_levels", {})
        }

    except Exception as e:
        print(f"⚠️ estimate_skill_levels failed: {e}")
        return {
            "hard_skill_levels": {skill: "unknown" for skill in hard_skills},
            "spoken_languages_levels": {lang: "unknown" for lang in spoken_languages}
        }


# ==== INDUSTRY MAPPING (LLM Call 4) ====
def map_industry(company_industry_str: str, llm_config: dict, industry_mapping: dict, prompts: dict) -> dict:

    if not company_industry_str.strip():
        return {"main_industry": "unknown", "subindustry": "unknown"}

    prompt_template = prompts.get("prompt_4", "")
    industry_mapping_str = json.dumps(industry_mapping, indent=2)
    prompt = (
        prompt_template
        .replace("{company_industry_str}", company_industry_str)
        .replace("{industry_mapping}", industry_mapping_str)
    )

    try:
        output_text = call_llm(prompt, llm_config)
        data = safe_json_parse(output_text)

        # Validate keys
        if "main_industry" not in data and "subindustry" not in data:
            print("⚠️ Missing keys in LLM response, defaulting to 'unknown'")
            return {"main_industry": "unknown", "subindustry": "unknown"}

        return data

    except Exception as e:
        print(f"⚠️ map_industry failed: {e}")
        return {"main_industry": "unknown", "subindustry": "unknown"}
