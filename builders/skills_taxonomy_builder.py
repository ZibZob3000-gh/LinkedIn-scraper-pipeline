import csv
import json
from typing import Optional
import pandas as pd

INPUT = "HR - R_Skills 1.csv"
OUT_JSON = "skill_to_info.json"
OUT_PARQUET = "skills_taxonomy.parquet"

rows = []
skipped_rows = []

with open(INPUT, newline='', encoding='utf-8') as f:
    reader = csv.reader(f, quotechar='"', skipinitialspace=True)
    header = next(reader, None)

    # If header doesn't look like a header (rare), we could treat it as data.
    # Common case: header contains "SkillID" etc. If it doesn't, the header remains skipped anyway.
    for lineno, r in enumerate(reader, start=2):  # start=2 to account for header line
        # strip whitespace from each cell, preserve empty strings
        r = [cell.strip() if cell is not None else "" for cell in r]

        # skip fully empty lines
        if len(r) == 0 or all(cell == "" for cell in r):
            continue

        # canonicalize based on length
        if len(r) == 4:
            skill_id, group, subgroup, skill = r
            subgroup = subgroup if subgroup != "" else None
        elif len(r) == 3:
            # The case you explicitly asked for: SkillID, SkillGroup, Skill (no subgroup)
            skill_id, group, skill = r
            subgroup = None
        elif len(r) > 4:
            # Best-effort: assume first 3 fields are id/group/subgroup and the rest form the skill (commas inside skill)
            skill_id = r[0]
            group = r[1] if len(r) > 1 else ""
            subgroup = r[2] if len(r) > 2 and r[2] != "" else None
            skill = ",".join(r[3:]).strip()
        else:
            # too few columns (e.g. 1 or 2) -> unreliable row, skip but record diagnostics
            skipped_rows.append((lineno, r))
            continue

        # If skill ended up empty but subgroup exists, assume the row was missing subgroup and values shifted:
        # (heuristic) — move subgroup to skill and set subgroup None.
        if (skill == "" or skill.lower() == "skill") and subgroup:
            skill = subgroup
            subgroup = None

        # If skill still empty -> skip but record
        if not skill:
            skipped_rows.append((lineno, [skill_id, group, subgroup, skill]))
            continue

        rows.append([skill_id, group, subgroup, skill])

# Build dataframe
columns = ["SkillID", "SkillGroup", "SkillSubGroup", "Skill"]
df = pd.DataFrame(rows, columns=columns)

# Build mapping: skill -> {ID, group, subgroup}
skill_to_info = {}
for _, row in df.iterrows():
    name = row["Skill"]
    skill_to_info[name] = {
        "ID": row["SkillID"],
        "group": row["SkillGroup"],
        "subgroup": row["SkillSubGroup"] if row["SkillSubGroup"] not in ("", None) else None
    }

# Save JSON
with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(skill_to_info, f, indent=2, ensure_ascii=False)

# Save Parquet if possible (won't crash script if engine missing)
try:
    df.to_parquet(OUT_PARQUET, index=False, engine="pyarrow")
    parquet_msg = f"Parquet saved to {OUT_PARQUET}"
except Exception as e:
    parquet_msg = f"Parquet not saved (engine missing or error): {e}"

# Print summary + small diagnostics so you can check quickly
print("✅ Done.")
print(f"Total CSV rows parsed into canonical rows: {len(rows)}")
print(f"Total mapped skills: {len(skill_to_info)}")
print(parquet_msg)

if skipped_rows:
    print("\n⚠️ Skipped rows (first 8 shown):")
    for lineno, r in skipped_rows[:8]:
        print(f"  line {lineno}: {r}")
else:
    print("\nNo skipped rows.")

# show a few sample mappings (first 8)
print("\nExamples (first 8 mapped skills):")
for i, (k, v) in enumerate(skill_to_info.items()):
    print(f"  {i+1}. '{k}' -> {v}")
    if i >= 7:
        break

# Helper lookup function
def get_skill_info(skill_name: str) -> Optional[dict]:
    """
    Return {ID, group, subgroup} for given skill name, or None if not found.
    """
    return skill_to_info.get(skill_name.strip(), None)
