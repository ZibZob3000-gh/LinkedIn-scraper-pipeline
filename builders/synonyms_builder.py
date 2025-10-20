import json

# ==== CONFIG ====
INPUT_FILE = "skill_synonyms.json"
OUTPUT_FILE = "skill_synonyms.json"  # you can overwrite INPUT_FILE if you want

# ==== LOAD JSON ====
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    skill_synonyms = json.load(f)

# ==== APPEND KEY TO VALUE LIST ====
for skill, synonyms in skill_synonyms.items():
    if skill not in synonyms:
        synonyms.insert(0, skill)  # insert at the beginning
    skill_synonyms[skill] = synonyms

# ==== SAVE UPDATED JSON ====
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(skill_synonyms, f, indent=2, ensure_ascii=False)

print(f"✅ Updated synonyms saved to {OUTPUT_FILE}")
