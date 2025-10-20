import json
import requests
import time
import re
import os

# ==== CONFIG ====
INPUT_FILE = "Descriptions.json"  # same folder as script
OUTPUT_FILE = "jobs_with_skills.json"
MODEL_NAME = "mistral:instruct"
MAX_RETRIES = 1

# ==== LOAD JOB DESCRIPTIONS ====
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    jobs = json.load(f)

results = []

# ==== PROCESS JOBS ====
for idx, job in enumerate(jobs, 1):
    description = job.get("description", "")

    prompt = f"""
You are an expert job recruiter.
Extract all skills from the following job description.
Categorize them into "hard_skills" and "soft_skills".
Return strictly valid JSON with **only keywords**, no full sentences, no extra commentary.
For hard_skills this really means only keywords (e.g. 'SQL').
For soft_skills this means as short as possible (still has to be clear).

JSON format:
{{
  "hard_skills": [],
  "soft_skills": []
}}

Job description:
{description}
"""

    skills = {"hard_skills": [], "soft_skills": []}  # default in case parsing fails

    # ==== SEND REQUEST WITH RETRIES ====
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": MODEL_NAME, "prompt": prompt},
                stream=True  # use streaming to capture partial responses
            )

            # ==== CONCATENATE STREAMED RESPONSES ====
            full_output = ""
            for chunk in response.iter_lines():
                if chunk:
                    decoded = chunk.decode("utf-8")
                    try:
                        json_chunk = json.loads(decoded)
                        full_output += json_chunk.get("response", "")
                    except json.JSONDecodeError:
                        full_output += decoded  # fallback for partial JSON

            print(f"\n--- RAW LLM OUTPUT FOR JOB {idx} ---\n{full_output}\n--- END RAW OUTPUT ---\n")

            # ==== CLEAN AND PARSE JSON ====
            match = re.search(r'{.*}', full_output, re.DOTALL)
            if match:
                skills = json.loads(match.group())

            break  # success
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for job {idx}: {e}")
            time.sleep(1)

    # ==== MERGE SKILLS WITH ORIGINAL JOB ====
    job_with_skills = {**job, **skills}
    results.append(job_with_skills)

    print(f"Processed {idx}/{len(jobs)} jobs")

# ==== SAVE RESULTS ====
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"\n✅ Finished! Results saved to {OUTPUT_FILE}")
