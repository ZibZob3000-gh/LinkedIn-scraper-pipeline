import json
import requests
import time
import re
import ast
from rapidfuzz import process, fuzz


# ==== TEXT CLEANUP HELPER ====
def clean_job_description(text: str) -> str:
    text = text.replace('\t', ' ')
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    text = '\n'.join(line.strip() for line in text.splitlines())
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


# ==== SKILL MAPPING ====
def map_skill_with_synonyms_verbose(
    extracted_skill: str,
    synonym_to_skill: dict,
    skill_lookup: dict,
    all_synonyms: list,
    fuzzy_threshold: int = 85
) -> dict:
    s_norm = extracted_skill.strip().lower()

    # Exact match
    if s_norm in synonym_to_skill:
        base_skill = synonym_to_skill[s_norm]
        return {"mapped_to": base_skill, "ID": skill_lookup.get(base_skill.lower(), {}).get("ID")}

    # Fuzzy match
    best = process.extractOne(s_norm, all_synonyms, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= fuzzy_threshold:
        base_skill = synonym_to_skill[best[0].lower()]
        return {"mapped_to": base_skill, "ID": skill_lookup.get(base_skill.lower(), {}).get("ID")}

    return {"mapped_to": None, "ID": None}


# ==== LOCAL LLM CALL ====
def call_llm(prompt: str, llm_config: dict) -> str:
    model_name = llm_config.get("model_name", "mistral:instruct")
    max_retries = llm_config.get("max_retries", 1)
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


# ==== SKILL + LANGUAGE EXTRACTION ====
def extract_skills(description: str, llm_config: dict, prompts: dict) -> dict:
    """
    Runs prompt_1: extracts hard/soft skills, department, and spoken languages.
    """
    prompt_template = prompts.get("prompt_1") or prompts.get("single_prompt", "")
    prompt = prompt_template.replace("{job_description}", description)

    output_text = call_llm(prompt, llm_config)
    try:
        match = re.search(r'{.*}', output_text, re.DOTALL)
        if match:
            json_str = match.group()
            data = json.loads(json_str)
            return {
                "hard_skills": data.get("hard_skills", []),
                "soft_skills": data.get("soft_skills", []),
                "spoken_languages": data.get("spoken_languages", []),
                "department": data.get("department", "null")
            }
    except Exception:
        pass
    return {"hard_skills": [], "soft_skills": [], "spoken_languages": [], "department": "null"}


# ==== LEVELS, CONTACT, LANGUAGE PROFICIENCY EXTRACTION ====
def estimate_skill_levels(description: str, hard_skills: list, spoken_languages: list, llm_config: dict, prompts: dict) -> dict:
    """
    Runs prompt_2: estimates proficiency levels for both hard skills and spoken languages,
    and extracts contact details.
    """
    prompt_template = prompts.get("prompt_2") or prompts.get("single_prompt", "")
    hard_skills_str = ", ".join(hard_skills)
    spoken_languages_str = ", ".join(spoken_languages)

    prompt = (
        prompt_template
        .replace("{job_description}", description)
        .replace("{hard_skills}", hard_skills_str)
        .replace("{spoken_languages}", spoken_languages_str)
    )

    output_text = call_llm(prompt, llm_config)
    try:
        match = re.search(r'{.*}', output_text, re.DOTALL)
        if match:
            json_str = match.group()
            data = json.loads(json_str)
            return {
                "skill_levels": data.get("skill_levels", {}),  # dict: {Skill: Level}
                "spoken_languages_levels": data.get("spoken_languages_levels", {}),  # dict: {Language: Level}
                "contact_details": data.get("contact_details", {
                    "name": "not provided",
                    "email": "not provided",
                    "phone_number": "not provided"
                }),
                "language_description": data.get("language_description", "not detected")
            }
    except Exception:
        pass

    # Fallback if LLM fails
    return {
        "skill_levels": {skill: "unknown" for skill in hard_skills},
        "spoken_languages_levels": {lang: "unknown" for lang in spoken_languages},
        "contact_details": {
            "name": "not provided",
            "email": "not provided",
            "phone_number": "not provided"
        },
        "language_description": "not detected"
    }
