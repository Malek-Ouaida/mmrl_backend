from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from mmrl.core.config.settings import settings

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """
    Minimal health check response.

    This endpoint is intentionally lightweight and side-effect free.
    """

    status: str
    environment: str


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health check",
)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        environment=settings.env,
    )
