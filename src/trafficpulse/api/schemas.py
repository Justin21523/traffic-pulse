from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class EmptyReason(BaseModel):
    code: str
    message: str
    suggestion: str | None = None


class ItemsResponse(BaseModel, Generic[T]):
    items: list[T] = Field(default_factory=list)
    reason: EmptyReason | None = None

