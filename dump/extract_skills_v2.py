import json
import requests
import time
from rapidfuzz import process, fuzz
import re
import ast

# ==== CONFIG ====
INPUT_FILE = "Descriptions.json"
SKILL_TAXONOMY_FILE = "skill_to_info.json"
SKILL_SYNONYMS_FILE = "skill_synonyms.json"
MODEL_NAME = "mistral:instruct"
MAX_RETRIES = 1
MAX_JOBS = 2
FUZZY_THRESHOLD = 85  # threshold for fuzzy matching

# ==== LOAD JOB DESCRIPTIONS ====
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    jobs = json.load(f)

# ==== LOAD SKILL TAXONOMY ====
with open(SKILL_TAXONOMY_FILE, "r", encoding="utf-8") as f:
    skill_to_info = json.load(f)

skill_lookup = {skill.lower(): info for skill, info in skill_to_info.items()}

# ==== LOAD SYNONYM MAPPING ====
with open(SKILL_SYNONYMS_FILE, "r", encoding="utf-8") as f:
    skill_synonyms = json.load(f)

# Flatten synonym dict for exact matching: synonym -> base skill
synonym_to_skill = {}
for base_skill, synonyms in skill_synonyms.items():
    for syn in synonyms:
        synonym_to_skill[syn.lower()] = base_skill

# Prepare fuzzy choices: list of all synonyms
all_synonyms = list(synonym_to_skill.keys())


# ==== HELPER FUNCTION (mapping) ====
def map_skill_with_synonyms_verbose(extracted_skill: str):
    """Return mapped skill, its ID, and placeholder for level."""
    s_norm = extracted_skill.strip().lower()

    # 1️⃣ Exact match in synonyms
    if s_norm in synonym_to_skill:
        base_skill = synonym_to_skill[s_norm]
        return {
            "mapped_to": base_skill,
            "ID": skill_lookup.get(base_skill.lower(), {}).get("ID"),
            "level": None  # will be filled later
        }

    # 2️⃣ Fuzzy match
    best = process.extractOne(
        s_norm,
        all_synonyms,
        scorer=fuzz.token_sort_ratio
    )
    if best and best[1] >= FUZZY_THRESHOLD:
        base_skill = synonym_to_skill[best[0].lower()]
        return {
            "mapped_to": base_skill,
            "ID": skill_lookup.get(base_skill.lower(), {}).get("ID"),
            "level": None
        }

    # 3️⃣ No match
    return {"mapped_to": None, "ID": None, "level": None}


# ==== HELPER: CALL LOCAL LLM ====
def call_llm(prompt: str, model_name: str = MODEL_NAME, max_retries: int = MAX_RETRIES):
    """Call local LLM API and return raw text output."""
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


# ==== STEP 1: SKILL EXTRACTION ====
def extract_skills(description: str):
    """Extract hard and soft skills from the job description."""
    with open("llm_prompt_extract_v2", "r", encoding="utf-8") as f:
        prompt_template = f.read()
    prompt = prompt_template.replace("{job_description}", description)

    output_text = call_llm(prompt)
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
    return {"hard_skills": [], "soft_skills": []}


# ==== STEP 2: LEVEL ESTIMATION ====
def estimate_skill_levels(description: str, hard_skills: list):
    """Estimate required skill level for each hard skill."""
    with open("llm_prompt_levels_v1", "r", encoding="utf-8") as f:
        prompt_template = f.read()

    # Prepare formatted input
    hard_skills_str = ", ".join(hard_skills)
    prompt = (
        prompt_template
        .replace("{job_description}", description)
        .replace("{hard_skills}", hard_skills_str)
    )

    output_text = call_llm(prompt)
    try:
        match = re.search(r'{.*}', output_text, re.DOTALL)
        if match:
            json_str = match.group()
            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                data = ast.literal_eval(json_str)
            return data.get("skill_levels", {})
    except Exception:
        pass
    return {skill: "unknown" for skill in hard_skills}


# ==== MAIN PROCESS LOOP ====
for idx, job in enumerate(jobs[:MAX_JOBS], 1):
    description = job.get("description", "")

    # ---- STEP 1: Extract skills ----
    skills = extract_skills(description)
    hard_skills = [s.strip() for s in skills.get("hard_skills", []) if s.strip()]
    soft_skills = [s.strip() for s in skills.get("soft_skills", []) if s.strip()]

    # ---- STEP 2: Estimate levels ----
    skill_levels = estimate_skill_levels(description, hard_skills)

    # ---- STEP 3: Map to taxonomy + combine ----
    skill_to_id_map = {}
    for skill_name in hard_skills:
        mapped_info = map_skill_with_synonyms_verbose(skill_name)
        mapped_info["level"] = skill_levels.get(skill_name, "unknown")
        skill_to_id_map[skill_name] = mapped_info

    # ---- BUILD FINAL RESULT ----
    result = {
        "job": job,
        "hard_skills": hard_skills,
        "soft_skills": soft_skills,
        "skill_to_id_map": skill_to_id_map
    }

    print(json.dumps(result, indent=2, ensure_ascii=False))

print("\n✅ Finished processing jobs!")
