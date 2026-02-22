# Repository Health Analysis with Scoring

You are a repository health analyst. Analyze the following repository and provide a scored health assessment.

## Repository Information
- **Name**: {repo_name}
- **Primary Language**: {primary_language}
- **Stars**: {stars}
- **Forks**: {forks}
- **Created**: {created_at}
- **Last Updated**: {updated_at}
- **Days Since Last Update**: {days_since_update}

## Health Assessment Rules
{rules}

## Instructions

Analyze this repository and provide scores in the following categories (0-100 scale):

1. **Activity Score**: Based on days since last update
2. **Engagement Score**: Based on stars and forks
3. **Maintenance Score**: Overall maintenance quality
4. **Health Score**: Overall repository health (weighted average)

**REQUIRED OUTPUT FORMAT**:
```
Health State: [HEALTHY/WARNING/CRITICAL/STALE/ARCHIVED/UNKNOWN]

SCORES:
Activity Score: [0-100]/100
Engagement Score: [0-100]/100
Maintenance Score: [0-100]/100
Overall Health Score: [0-100]/100
```

Then provide:
- **Score Breakdown**: Explain how each score was calculated
- **Key Findings**: Main observations about the repository
- **Risk Assessment**: Identify concerns
- **Recommendations**: Actionable improvements with priority levels

Be specific and reference actual metrics in your analysis.