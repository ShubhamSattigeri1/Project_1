from run_pipeline import run_pipeline

owner = input("GitHub username: ")
repo = input("Repository name: ")

report = run_pipeline(owner, repo)

print("\n===== REPORT =====\n")
print(report)