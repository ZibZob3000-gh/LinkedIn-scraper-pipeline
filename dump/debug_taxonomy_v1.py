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
MAX_JOBS = 3
FUZZY_THRESHOLD = 70  # threshold for fuzzy matching

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

# ==== HELPER FUNCTION ====
def map_skill_with_synonyms_verbose(extracted_skill: str):
    s_norm = extracted_skill.strip().lower()

    # 1️⃣ Exact match in synonyms
    if s_norm in synonym_to_skill:
        base_skill = synonym_to_skill[s_norm]
        return {"mapped_to": base_skill, "ID": skill_lookup.get(base_skill.lower(), {}).get("ID")}

    # 2️⃣ Fuzzy match
    best = process.extractOne(
        s_norm,
        all_synonyms,
        scorer=fuzz.token_sort_ratio
    )
    if best and best[1] >= FUZZY_THRESHOLD:
        base_skill = synonym_to_skill[best[0].lower()]
        return {"mapped_to": base_skill, "ID": skill_lookup.get(base_skill.lower(), {}).get("ID")}

    # 3️⃣ No match
    return {"mapped_to": None, "ID": None}

# ==== PROCESS JOBS ====
for idx, job in enumerate(jobs[:MAX_JOBS], 1):
    description = job.get("description", "")

    # ==== LOAD LLM PROMPT TEMPLATE ====
    with open("llm_prompt", "r", encoding="utf-8") as f:
        llm_prompt_template = f.read()

    # Fill in job description
    prompt = llm_prompt_template.replace("{job_description}", description)

    # Default fallback
    skills = {"hard_skills": [], "soft_skills": []}

    # ==== LLM CALL ====
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": MODEL_NAME, "prompt": prompt},
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

            # ==== DEBUG PRINT: RAW LLM OUTPUT ====
            print(f"\n--- DEBUG: RAW LLM OUTPUT for job {idx} ---")
            print(full_output)
            print("--- END DEBUG ---\n")

            # ==== SAFE JSON PARSING ====
            try:
                match = re.search(r'{.*?}', full_output, re.DOTALL)
                if match:
                    json_str = match.group()
                    print(f"\n--- DEBUG: EXTRACTED JSON BLOCK for job {idx} ---")
                    print(json_str)
                    print("--- END DEBUG ---\n")
                    try:
                        skills = json.loads(json_str)
                    except json.JSONDecodeError:
                        skills = ast.literal_eval(json_str)
                else:
                    print(f"⚠️ No JSON block found in LLM output for job {idx}")
            except Exception as e:
                print(f"⚠️ Failed to parse LLM output for job {idx}: {e}")
                skills = {"hard_skills": [], "soft_skills": []}

            break
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for job {idx}: {e}")
            time.sleep(1)

    # ==== CLEAN SKILLS ====
    hard_skills = [s.strip() for s in skills.get("hard_skills", []) if s.strip()]
    soft_skills = [s.strip() for s in skills.get("soft_skills", []) if s.strip()]

    # ==== MAP HARD SKILLS TO IDS (verbose) ====
    skill_to_id_map = {s: map_skill_with_synonyms_verbose(s) for s in hard_skills}

    # ==== BUILD RESULT ====
    result = {
        "job": job,
        "hard_skills": hard_skills,
        "soft_skills": soft_skills,
        "skill_to_id_map": skill_to_id_map
    }

    print(f"\n--- JOB {idx} RESULTS ---")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"--- END JOB {idx} ---\n")

print("\n✅ Finished processing jobs!")
