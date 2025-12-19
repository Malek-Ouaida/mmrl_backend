from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from mmrl.core.config.settings import settings
from mmrl.core.run.manager import RunManager

router = APIRouter(tags=["runs"])


class CreateRunRequest(BaseModel):
    """
    Request payload for creating a new run.
    """

    seed: int | None = Field(
        default=None,
        description="Optional RNG seed override for reproducibility",
    )


class CreateRunResponse(BaseModel):
    """
    Response returned after successfully creating a run.
    """

    run_id: str = Field(
        ...,
        description="Unique identifier of the created run",
    )


@router.post(
    "/runs",
    response_model=CreateRunResponse,
    summary="Create a new run",
)
def create_run(payload: CreateRunRequest) -> CreateRunResponse:
    seed = payload.seed if payload.seed is not None else settings.default_seed

    manager = RunManager(settings.runs_dir)
    run = manager.create_run(
        seed=seed,
        config_snapshot=settings.model_dump(),
    )

    return CreateRunResponse(run_id=run.run_id)
