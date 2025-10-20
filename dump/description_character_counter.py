import json
import re

# ==== TEXT CLEANUP HELPER ====
def clean_job_description(text: str) -> str:
    """Normalize whitespace: remove redundant newlines and spaces while keeping paragraphs."""
    text = text.replace('\t', ' ')
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    text = '\n'.join(line.strip() for line in text.splitlines())
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


# ==== LOAD JSON FILE ====
with open("Descriptions.json", "r", encoding="utf-8") as f:
    descriptions_data = json.load(f)

# ==== PARAMETER: HOW MANY DESCRIPTIONS TO PROCESS ====
num_descriptions_to_process = 300  # change this to whatever number you want
descriptions_data = descriptions_data[:num_descriptions_to_process]

# ==== CLEAN AND COMPUTE CHARACTER COUNTS ====
char_counts = []

for entry in descriptions_data:
    desc = entry.get("description", "")
    cleaned = clean_job_description(desc)
    char_counts.append(len(cleaned))

# ==== COMPUTE AVERAGE ====
if char_counts:
    average_chars = sum(char_counts) / len(char_counts)
    print(f"Processed {len(char_counts)} descriptions")
    print(f"Average character count (after preprocessing): {average_chars:.2f}")
else:
    print("No descriptions found in the file.")
