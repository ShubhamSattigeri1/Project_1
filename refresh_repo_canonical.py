import requests
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  
OWNER = "ShubhamSattigeri1"
REPO = "JSPM"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "Sattigeri@Maang50"),
    "database": os.getenv("DB_NAME", "hinduja_group")
}

def fetch_repo_metadata():
    url = f"https://api.github.com/repos/{OWNER}/{REPO}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def normalize_repo(data):
    return {
        "repo_name": data["name"],
        "primary_language": data["language"],
        "stars": data["stargazers_count"],
        "forks": data["forks_count"],
        "created_at": data["created_at"],
        "updated_at": data["updated_at"]
    }

def parse_github_datetime(dt_str):
    if not dt_str:
        return None
    return datetime.fromisoformat(dt_str.rstrip('Z'))

def upsert_repo_canonical(repo):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql = """
    INSERT INTO repo_canonical (
        repo_name,
        primary_language,
        stars,
        forks,
        created_at,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        primary_language = VALUES(primary_language),
        stars = VALUES(stars),
        forks = VALUES(forks),
        updated_at = VALUES(updated_at)
    """
    
    # Parse the datetimes
    created_at_parsed = parse_github_datetime(repo["created_at"])
    updated_at_parsed = parse_github_datetime(repo["updated_at"])
    
    cursor.execute(sql, (
        repo["repo_name"],
        repo["primary_language"],
        repo["stars"],
        repo["forks"],
        created_at_parsed,
        updated_at_parsed  
    ))
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    raw = fetch_repo_metadata()
    repo = normalize_repo(raw)
    upsert_repo_canonical(repo)
    print("repo_canonical refreshed successfully")

if __name__ == "__main__":
    main()