from datetime import datetime, timezone
from typing import Any, Literal
from pydantic import BaseModel, Field, ConfigDict

StageStatus = Literal["pending", "running", "completed", "failed"]

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class DocumentSubmit(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    title: str = Field(min_length=1, max_length=300)
    content: str = Field(min_length=1, max_length=200_000)
    client_doc_ref: str | None = Field(default=None, min_length=1, max_length=256)

class DocumentPatch(BaseModel):
    content: str = Field(min_length=1, max_length=200_000)
    expected_version: int | None = Field(default=None, ge=1)

class StageView(BaseModel):
    status: StageStatus
    attempts: int = 0
    error: str | None = None
    content_hash: str | None = None

class DocumentView(BaseModel):
    id: str
    user_id: str
    title: str
    content: str
    status: str
    version: int
    content_hash: str
    stages: dict[str, StageView]
    summary: str | None = None
    tags: list[str] | None = None
    summary_hash: str | None = None
    tags_hash: str | None = None
    client_doc_ref: str | None = None
    created_at: datetime
    updated_at: datetime
    result_is_current: bool
    model_config = ConfigDict(extra="ignore")
