import json
import requests
import time
import re
import ast
from rapidfuzz import process, fuzz


# ==== TEXT CLEANUP HELPER ====
def clean_job_description(text: str) -> str:
    text = text.replace('\t', ' ').replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    text = '\n'.join(line.strip() for line in text.splitlines())
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


# ==== SAFE JSON PARSER ====
def safe_json_parse(text: str) -> dict:
    """Safely extract and repair JSON from possibly malformed LLM output."""
    print("🔍 Original LLM output length:", len(text))

    # ===== Step 0: Clean obvious non-JSON artifacts =====
    # Remove // comments that are NOT part of URLs or inside strings
    text_clean = re.sub(r'(?<!:)//\s.*$', '', text, flags=re.MULTILINE)

    # Remove (optional) or similar tags inside parentheses
    text_clean = re.sub(r'\(".*?"\)|\s*\(.*?\)', '', text_clean)

    # Remove <tags> or angle-bracket metadata
    text_clean = re.sub(r'<.*?>', '', text_clean)

    # ===== Step 1: Extract JSON block =====
    json_match = re.search(r'{.*}', text_clean, re.DOTALL)
    if json_match:
        json_str = json_match.group()
        try:
            print("✅ Step 1: Direct JSON extraction succeeded.")
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"⚠️ Step 1 JSON decode failed: {e}. Trying repair...")

    # ===== Step 2: Smart reconstruction =====
    start_idx = text_clean.find('{')
    if start_idx == -1:
        raise ValueError("❌ No JSON object found in LLM output")

    json_str = text_clean[start_idx:].strip()
    lines = json_str.splitlines()
    json_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith('"') or any(stripped.endswith(x) for x in ['}', ']', ',', '{', '[']):
            # Remove parentheses comments again
            line_clean = re.sub(r'\s*\(.*?\)', '', line)
            # Remove inline comments (but not URLs)
            line_clean = re.sub(r'(?<!:)//\s.*$', '', line_clean)
            json_lines.append(line_clean)
        else:
            # Stop if clearly not JSON anymore
            break

    if not json_lines:
        raise ValueError("❌ No JSON-like content found in LLM output")

    repaired_json_str = "\n".join(json_lines)
    print("🔍 Reconstructed JSON string (before adding '}'):\n", repaired_json_str)

    # Ensure it ends with a closing brace
    if not repaired_json_str.rstrip().endswith('}'):
        repaired_json_str = repaired_json_str.rstrip() + "\n}"
        print("⚠️ Added missing closing '}' at the end.")

    # Remove trailing commas before } or ]
    repaired_json_str = re.sub(r',(\s*[}\]])', r'\1', repaired_json_str)
    print("🔍 JSON string after removing trailing commas:\n", repaired_json_str)

    # ===== Step 3: Parse safely =====
    try:
        parsed = json.loads(repaired_json_str)
        print("✅ JSON parsed successfully.")
        return parsed
    except json.JSONDecodeError as e:
        print(f"⚠️ JSON strict parse failed: {e}. Trying ast.literal_eval...")

        # Replace true/false/null to Python equivalents for literal_eval
        alt_str = repaired_json_str.replace("true", "True").replace("false", "False").replace("null", "None")
        try:
            parsed = ast.literal_eval(alt_str)
            print("✅ Parsed successfully with ast.literal_eval.")
            return parsed
        except Exception as e2:
            print(f"❌ JSON repair failed: {e2}")
            raise


# ==== SKILL MAPPING ====
def map_skill_with_synonyms_verbose(
    extracted_skill: str,
    synonym_to_skill: dict,
    skill_lookup: dict,
    all_synonyms: list,
    fuzzy_threshold: int = 85
) -> dict:
    s_norm = extracted_skill.strip().lower()
    print(f"\n🔍 Mapping skill: '{extracted_skill}' (normalized: '{s_norm}')")

    if s_norm in synonym_to_skill:
        base_skill = synonym_to_skill[s_norm]
        skill_id = skill_lookup.get(base_skill.lower(), {}).get("ID")
        print(f"✅ Exact match: '{s_norm}' → '{base_skill}' (ID={skill_id})")
        return {"mapped_to": base_skill, "ID": skill_id}

    best = process.extractOne(s_norm, all_synonyms, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= fuzzy_threshold:
        base_skill = synonym_to_skill[best[0].lower()]
        skill_id = skill_lookup.get(base_skill.lower(), {}).get("ID")
        print(f"🤖 Fuzzy match: '{s_norm}' → '{base_skill}' (sim={best[1]}, ID={skill_id})")
        return {"mapped_to": base_skill, "ID": skill_id}

    print(f"❌ No match found for: '{s_norm}'")
    return {"mapped_to": None, "ID": None}


# ==== LOCAL LLM CALL WITH DEBUGGING ====
def call_llm(prompt: str, llm_config: dict) -> str:
    model_name = llm_config.get("model_name", "mistral:instruct")
    max_retries = llm_config.get("max_retries", 1)

    print(f"➡️ Calling LLM '{model_name}' with prompt (first 200 chars):\n{prompt[:200]}...\n")

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": model_name, "prompt": prompt},
                stream=True,
                timeout=30
            )

            if response.status_code != 200:
                print(f"⚠️ Attempt {attempt}: LLM returned status code {response.status_code}")
                time.sleep(1)
                continue

            full_output = ""
            for chunk in response.iter_lines():
                if chunk:
                    decoded = chunk.decode("utf-8")
                    try:
                        json_chunk = json.loads(decoded)
                        full_output += json_chunk.get("response", "")
                    except json.JSONDecodeError:
                        full_output += decoded

            print(f"✅ LLM returned output of length {len(full_output)}")
            print(f"🔍 Full LLM output:\n{full_output}\n")

            if not full_output.strip():
                print(f"⚠️ Attempt {attempt}: LLM returned empty output")
            else:
                return full_output

        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            time.sleep(1)

    raise RuntimeError(f"❌ LLM call failed after {max_retries} attempts. Check that the model '{model_name}' is installed and running.")


# ==== SKILL + LANGUAGE EXTRACTION ====
def extract_skills(description: str, llm_config: dict, prompts: dict) -> dict:
    prompt_template = prompts.get("prompt_1") or prompts.get("single_prompt", "")
    prompt = prompt_template.replace("{job_description}", description)

    try:
        output_text = call_llm(prompt, llm_config)
        data = safe_json_parse(output_text)  # <-- robust parsing here
        print(f"🔍 JSON extracted from LLM output:\n{json.dumps(data, indent=2)}\n")

        return {
            "hard_skills": data.get("hard_skills", []),
            "soft_skills": data.get("soft_skills", []),
            "spoken_languages": data.get("spoken_languages", []),
            "department": data.get("department", "null")
        }

    except Exception as e:
        print(f"⚠️ extract_skills failed: {e}")

    print("⚠️ Using fallback for extract_skills")
    return {"hard_skills": [], "soft_skills": [], "spoken_languages": [], "department": "null"}


# ==== LEVELS, CONTACT, LANGUAGE PROFICIENCY EXTRACTION ====
def estimate_skill_levels(description: str, hard_skills: list, spoken_languages: list, llm_config: dict, prompts: dict) -> dict:
    prompt_template = prompts.get("prompt_2") or prompts.get("single_prompt", "")
    hard_skills_str = ", ".join(hard_skills)
    spoken_languages_str = ", ".join(spoken_languages)
    prompt = prompt_template.replace("{job_description}", description)\
                            .replace("{hard_skills}", hard_skills_str)\
                            .replace("{spoken_languages}", spoken_languages_str)

    try:
        output_text = call_llm(prompt, llm_config)
        data = safe_json_parse(output_text)  # <-- robust parsing here
        print(f"🔍 JSON extracted from LLM output:\n{json.dumps(data, indent=2)}\n")

        return {
            "skill_levels": data.get("hard_skill_levels", {}),
            "spoken_languages_levels": data.get("spoken_languages_levels", {}),
            "contact_details": data.get("contact_details", {
                "name": "not provided",
                "email": "not provided",
                "phone_number": "not provided"
            }),
            "language_description": data.get("language_description", "not detected")
        }

    except Exception as e:
        print(f"⚠️ estimate_skill_levels failed: {e}")

    print("⚠️ Using fallback for estimate_skill_levels")
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
