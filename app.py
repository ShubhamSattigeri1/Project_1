from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from .run_pipeline import run_pipeline

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RepoRequest(BaseModel):
    owner: str
    repo: str
    consent: bool

@app.post("/analyze")
async def analyze_repo(req: RepoRequest):

    if not req.consent:
        raise HTTPException(status_code=400, detail="Consent required")

    try:
        result = await run_pipeline(req.owner, req.repo)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# python -m uvicorn pipeline.app:app --reload
# python -m uvicorn pipeline.app:app --reload