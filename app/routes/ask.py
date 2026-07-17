from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.ai.nl_endpoint import handle_ask  # will be added next

router = APIRouter(tags=["ai"])


class AskRequest(BaseModel):
    question: str


@router.post("/ask")
async def ask(req: AskRequest):
    if not req.question or not req.question.strip():
        raise HTTPException(status_code=400, detail="question is required")

    return await handle_ask(req.question)
