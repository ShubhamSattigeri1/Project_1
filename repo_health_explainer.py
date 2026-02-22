import yaml
import mysql.connector
from datetime import datetime, date
import os
import asyncio
from groq import AsyncGroq
from google import genai
from pathlib import Path
from openai import OpenAI
from google.genai import types
# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Sattigeri@Maang50",
    "database": "hinduja_group"
}
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = AsyncGroq(
    api_key=GROQ_API_KEY,  # This is the default and can be omitted
)
# RULES_PATH = Path("config/repo_health_rules.yaml")
PROMPT_PATH = Path("repo_health_prompt.md")
# ---------- LOAD RULES --------------
with open('repo_health_rules.yaml', "r") as f:              
    rules = yaml.safe_load(f)            
# ---------- LOAD PROMPT -------------        
prompt_template = PROMPT_PATH.read_text()
# ---------- FETCH ONE REPO ----------
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)
cursor.execute("SELECT * FROM repo_canonical LIMIT 1;")
repo = cursor.fetchone()
cursor.close()
conn.close()
# ---------- COMPUTE DAYS SINCE UPDATE ----------
updated_at = repo["updated_at"].date()
days_since_update = (date.today() - updated_at).days
# ---------- FILL PROMPT ----------
filled_prompt = prompt_template.format(
    repo_name=repo["repo_name"],
    primary_language=repo["primary_language"],
    stars=repo["stars"],
    forks=repo["forks"],
    created_at=repo["created_at"],
    updated_at=repo["updated_at"],
    days_since_update=days_since_update,
    rules=yaml.dump(rules, sort_keys=False),
    timeline="",          # or some precomputed text
    current_state="",     # or some precomputed text
    candidate_name = " ",
    problem_name = " ",
    total_score = " ",
    total_marks = " ",
    score_table = " ",
    correctness_pct = " ",
    execution_time_ms = " ",
    memory_mb = " ",
    stress_passed = " ",
    invalid_passed = " "
)
# ---------- CALL LLM ----------
async def run_groq():
    chat_completion = await client.chat.completions.create(  # ← Fixed: chat (singular)
    model="llama-3.3-70b-versatile",
    messages=[
        {
            "role": "system", 
            "content": "You are a precise and example reasoning assistant."
        },
        {
            "role": "user", 
            "content": filled_prompt
        }
    ],
    temperature=0.1,
)
    return chat_completion
if __name__ == "__main__":
    chat_completion = asyncio.run(run_groq())

# print(chat_completion.choices[0].message.content)  # This is CORRECT
if chat_completion is None:
    print("API call failed: response is None. Check params, API key, or rate limits.")
else:
    print(chat_completion.choices[0].message.content)

# ---------- PARSE AI OUTPUT ----------
# ---------- PARSE AI OUTPUT (HEALTH STATE + FULL REPORT) ----------
report_output = chat_completion.choices[0].message.content
lines = chat_completion.choices[0].message.content.splitlines()

health_state = None
explanation_lines = []

for line in lines:
    stripped = line.strip()
    if stripped.lower().startswith("health state:"):
        health_state = stripped.split(":", 1)[1].strip()
    elif stripped != "":
        explanation_lines.append(line)

explanation = " ".join(explanation_lines).strip()

# Fallback safety
if not health_state:
    health_state = "UNKNOWN"

# ---------- SINGLE DB CONNECTION FOR ALL WRITES ----------
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# ---------- INSERT INTO repo_insights ----------
insert_sql = """
INSERT INTO repo_insights (
    repo_id,
    health_state,
    explanation,
    days_since_update,
    rules_version,
    model_name
)
VALUES (%s, %s, %s, %s, %s, %s)
"""

cursor.execute(
    insert_sql,
    (
        repo["repo_id"],
        health_state,
        explanation,
        days_since_update,
        "v1.0",
        "llama-3.3-70b-versatile"
    )
)

# ---------- INSERT INTO repo_health_timeline ----------
timeline_sql = """
INSERT INTO repo_health_timeline (
    repo_id,
    health_state,
    period_date,
    explanation
)
VALUES (%s, %s, CURDATE(), %s)
"""

cursor.execute(
    timeline_sql,
    (
        repo["repo_id"],
        health_state,
        explanation
    )
)

# ---------- INSERT FULL PROFESSIONAL REPORT ----------
insert_report_sql = """
INSERT INTO repo_reports (repo_id, report_text)
VALUES (%s, %s)
"""

cursor.execute(
    insert_report_sql,
    (
        repo["repo_id"],
        report_output
    )
)

conn.commit()
cursor.close()
conn.close()






