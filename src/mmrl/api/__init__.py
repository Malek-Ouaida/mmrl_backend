from __future__ import annotations

from fastapi import APIRouter

from mmrl.api.routes.health import router as health_router
from mmrl.api.routes.runs import router as runs_router

# Top-level API router
router = APIRouter()

# Route composition
router.include_router(health_router)
router.include_router(runs_router)
