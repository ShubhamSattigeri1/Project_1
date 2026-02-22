import yaml
import mysql.connector
import requests
from datetime import date
import os
import asyncio
from groq import AsyncGroq
from pathlib import Path
from datetime import datetime, date

def parse_github_datetime(dt_str):
    if not dt_str:
        return None
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Sattigeri@Maang50",
    "database": "hinduja_group"
}

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

client = AsyncGroq(api_key=GROQ_API_KEY)
BASE_DIR = Path(__file__).resolve().parent

PROMPT_PATH = BASE_DIR / "repo_health_prompt.md"
RULES_PATH = BASE_DIR / "repo_health_rules.yaml"

with open(RULES_PATH, "r") as f:
    rules = yaml.safe_load(f)

prompt_template = PROMPT_PATH.read_text()


# ---------- STEP 1: FETCH GITHUB DATA ----------
def get_timeline(repo_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT period_date, health_state
        FROM repo_health_timeline
        WHERE repo_id = %s
        ORDER BY period_date ASC
    """, (repo_id,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows


def fetch_repo(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Repository not found")

    return response.json()

# ---------- STEP 2: UPSERT INTO DB ----------
def upsert_repo(data):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)   # ✅ IMPORTANT

    sql = """
    INSERT INTO repo_canonical (
        repo_name, primary_language, stars, forks, created_at, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        primary_language = VALUES(primary_language),
        stars = VALUES(stars),
        forks = VALUES(forks),
        updated_at = VALUES(updated_at)
    """

    created_at = parse_github_datetime(data["created_at"])
    updated_at = parse_github_datetime(data["updated_at"])

    cursor.execute(sql, (
        data["name"],
        data["language"],
        data["stargazers_count"],
        data["forks_count"],
        created_at,
        updated_at
    ))

    conn.commit()

    # ✅ GET LATEST INSERTED ROW
    cursor.execute(
        "SELECT * FROM repo_canonical WHERE repo_name=%s ORDER BY repo_id DESC LIMIT 1",
        (data["name"],)
    )

    repo = cursor.fetchone()

    cursor.close()
    conn.close()

    return repo

    
# ---------- STEP 3: BUILD PROMPT ----------
def build_prompt(repo):
    updated_at = repo["updated_at"]

    if isinstance(updated_at, datetime):
        updated_at_date = updated_at.date()
    else:
        updated_at_date = updated_at

    days_since_update = (date.today() - updated_at_date).days

    timeline_data = get_timeline(repo["repo_id"])

    prompt = prompt_template.format(
        repo_name=repo["repo_name"],
        primary_language=repo["primary_language"],
        stars=repo["stars"],
        forks=repo["forks"],
        created_at=repo["created_at"],
        updated_at=repo["updated_at"],
        days_since_update=days_since_update,
        rules=yaml.dump(rules, sort_keys=False),
        timeline=str(timeline_data),
        current_state=""
    )

    return prompt, days_since_update   # ✅ FIX

# ---------- STEP 4: CALL LLM ----------
async def call_llm(prompt):
    res = await client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": "Use only provided data."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1
    )
    return res.choices[0].message.content


# ---------- STEP 5: PARSE ----------
def parse_output(text):
    health_state = "UNKNOWN"
    lines = text.splitlines()

    for line in lines:
        if line.lower().startswith("health state:"):
            health_state = line.split(":")[1].strip()

    return health_state, text


# ---------- STEP 6: STORE ----------
def store(repo_id, health_state, explanation, days_since_update):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO repo_insights (
        repo_id, health_state, explanation, days_since_update, rules_version, model_name
    ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (repo_id, health_state, explanation, days_since_update, "v1", "groq"))

    cursor.execute("""
    INSERT INTO repo_health_timeline (
        repo_id, health_state, period_date, explanation
    ) VALUES (%s, %s, CURDATE(), %s)
    """, (repo_id, health_state, explanation))

    cursor.execute("""
    INSERT INTO repo_reports (repo_id, report_text)
    VALUES (%s, %s)
    """, (repo_id, explanation))

    conn.commit()
    cursor.close()
    conn.close()

# ---------- MAIN PIPELINE ----------
async def run_pipeline(owner, repo_name):
    try:
        data = fetch_repo(owner, repo_name)

        repo = upsert_repo(data)

        if repo is None:
            raise Exception("Failed to store repo")

        prompt, days_since_update = build_prompt(repo)

        result = await call_llm(prompt)

        health_state, report = parse_output(result)

        store(repo["repo_id"], health_state, report, days_since_update)

        # ✅ FETCH TIMELINE HERE
        timeline = get_timeline(repo["repo_id"])

        return {
            "repo_name": repo_name,
            "health_state": health_state,
            "report": report,
            "metrics": {
                "stars": repo["stars"],
                "forks": repo["forks"],
                "days_since_update": days_since_update,
                "language": repo["primary_language"],
            },
            "timeline": timeline   # ✅ FIXED
        }

    except Exception as e:
        print("ERROR:", e)   # 🔥 VERY IMPORTANT FOR DEBUG
        raise Exception(f"Pipeline error: {str(e)}")
    
