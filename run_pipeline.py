import yaml
import psycopg2
import requests
from datetime import datetime, date
import os
import asyncio
from groq import AsyncGroq
from pathlib import Path

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

BASE_DIR = Path(__file__).resolve().parent
PROMPT_PATH = BASE_DIR / "repo_health_prompt.md"
RULES_PATH = BASE_DIR / "repo_health_rules.yaml"

# ---------- LOAD RULES ----------
with open(RULES_PATH, "r") as f:
    rules = yaml.safe_load(f)

prompt_template = PROMPT_PATH.read_text()

# ---------- DB CONNECTION ----------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------- FETCH GITHUB ----------
def fetch_repo(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}"
    res = requests.get(url)

    if res.status_code != 200:
        raise Exception(f"GitHub API error: {res.text}")

    return res.json()


# ---------- UPSERT ----------
def upsert_repo(data):
    conn = get_conn()
    cursor = conn.cursor()

    created_at = datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%SZ")
    updated_at = datetime.strptime(data["updated_at"], "%Y-%m-%dT%H:%M:%SZ")

    sql = """
    INSERT INTO repo_canonical (
        repo_name, primary_language, stars, forks, created_at, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (repo_name) DO UPDATE SET
        primary_language = EXCLUDED.primary_language,
        stars = EXCLUDED.stars,
        forks = EXCLUDED.forks,
        updated_at = EXCLUDED.updated_at
    RETURNING repo_id;
    """

    cursor.execute(sql, (
        data["name"],
        data["language"],
        data["stargazers_count"],
        data["forks_count"],
        created_at,
        updated_at
    ))

    repo_id = cursor.fetchone()[0]
    conn.commit()

    # fetch full row
    cursor.execute("SELECT * FROM repo_canonical WHERE repo_id=%s", (repo_id,))
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    return {
        "repo_id": row[0],
        "repo_name": row[1],
        "primary_language": row[2],
        "stars": row[3],
        "forks": row[4],
        "created_at": row[5],
        "updated_at": row[6]
    }


# ---------- TIMELINE ----------
def get_timeline(repo_id):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT period_date, health_state
        FROM repo_health_timeline
        WHERE repo_id = %s
        ORDER BY period_date ASC
    """, (repo_id,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [{"date": str(r[0]), "state": r[1]} for r in rows]


# ---------- PROMPT ----------
def build_prompt(repo):
    updated_date = repo["updated_at"].date()
    days_since_update = (date.today() - updated_date).days

    timeline = get_timeline(repo["repo_id"])

    prompt = prompt_template.format(
        repo_name=repo["repo_name"],
        primary_language=repo["primary_language"],
        stars=repo["stars"],
        forks=repo["forks"],
        created_at=repo["created_at"],
        updated_at=repo["updated_at"],
        days_since_update=days_since_update,
        rules=yaml.dump(rules, sort_keys=False),
        timeline=str(timeline),
        current_state=""
    )

    return prompt, days_since_update, timeline


# ---------- LLM ----------
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


# ---------- PARSE ----------
def parse_output(text):
    state = "UNKNOWN"

    for line in text.splitlines():
        if line.lower().startswith("health state:"):
            state = line.split(":")[1].strip()

    return state, text


# ---------- STORE ----------
def store(repo_id, state, report, days):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO repo_insights (
        repo_id, health_state, explanation, days_since_update, rules_version, model_name
    ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (repo_id, state, report, days, "v1", "groq"))

    cursor.execute("""
    INSERT INTO repo_health_timeline (
        repo_id, health_state, period_date, explanation
    ) VALUES (%s, %s, CURRENT_DATE, %s)
    """, (repo_id, state, report))

    cursor.execute("""
    INSERT INTO repo_reports (repo_id, report_text)
    VALUES (%s, %s)
    """, (repo_id, report))

    conn.commit()
    cursor.close()
    conn.close()


# ---------- MAIN ----------
async def run_pipeline(owner, repo_name):
    data = fetch_repo(owner, repo_name)
    repo = upsert_repo(data)

    prompt, days, timeline = build_prompt(repo)

    output = await call_llm(prompt)

    state, report = parse_output(output)

    store(repo["repo_id"], state, report, days)

    return {
        "repo_name": repo_name,
        "health_state": state,
        "report": report,
        "metrics": {
            "stars": repo["stars"],
            "forks": repo["forks"],
            "days_since_update": days,
            "language": repo["primary_language"],
        },
        "timeline": timeline
    }