from __future__ import annotations

from datetime import datetime
from threading import Lock

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from mmrl.core.config.settings import settings
from mmrl.core.run.artifacts import artifacts_for
from mmrl.core.run.factory import RunFactory
from mmrl.core.run.manager import RunManager
from mmrl.core.run.registry import RunRegistry, RunStatus
from mmrl.core.run.spec import RunSpec

router = APIRouter(tags=["runs"])

# Minimal singleton for now (works in single-process dev).
_registry = RunRegistry()

# Live handles in this process only (dev-mode)
_live_lock = Lock()
_live: dict[str, "RunHandle"] = {}

# Import type only (avoid circulars)
from mmrl.core.run.assembly import RunHandle  # noqa: E402  (safe in runtime too)


# =========================
# Schemas
# =========================

class CreateRunRequest(BaseModel):
    seed: int | None = Field(default=None, description="Optional RNG seed override")
    run_spec: RunSpec | None = Field(default=None, description="Optional RunSpec override")


class CreateRunResponse(BaseModel):
    run_id: str


class StartRunResponse(BaseModel):
    run_id: str
    status: RunStatus


class StopRunResponse(BaseModel):
    run_id: str
    status: RunStatus


class RunDetailsResponse(BaseModel):
    run_id: str
    status: RunStatus
    created_at_utc: datetime
    updated_at_utc: datetime
    run_dir: str
    artifacts: dict[str, str]
    error_type: str | None = None
    error_message: str | None = None


class RunsListResponse(BaseModel):
    runs: list[RunDetailsResponse]


# =========================
# Routes
# =========================

@router.post("/runs", response_model=CreateRunResponse)
def create_run(payload: CreateRunRequest) -> CreateRunResponse:
    seed = payload.seed if payload.seed is not None else settings.default_seed

    manager = RunManager(settings.runs_dir)
    run = manager.create_run(seed=seed, config_snapshot=settings.model_dump())

    _registry.upsert_created(run)

    # Ensure artifacts exist (idempotent)
    art = artifacts_for(runs_dir=settings.runs_dir, run_id=run.run_id)
    art.ensure_dirs()
    art.events_jsonl.touch(exist_ok=True)

    # Persist RunSpec as canonical config.json
    factory = RunFactory(runs_dir=settings.runs_dir)

    if payload.run_spec is None:
        # Default spec, but seed should reflect what the run was created with
        spec = RunSpec(seed=seed)
    else:
        # Respect caller, but keep seed aligned unless explicitly set
        spec = payload.run_spec
        if spec.seed is None:
            spec.seed = seed  # pydantic model is mutable by default

    factory.save_spec(run_id=run.run_id, spec=spec)

    return CreateRunResponse(run_id=run.run_id)


@router.post("/runs/{run_id}/start", response_model=StartRunResponse)
def start_run(run_id: str) -> StartRunResponse:
    art = artifacts_for(runs_dir=settings.runs_dir, run_id=run_id)
    if not art.run_dir.exists():
        raise HTTPException(status_code=404, detail="run not found")

    factory = RunFactory(runs_dir=settings.runs_dir)

    with _live_lock:
        handle = _live.get(run_id)
        if handle is None:
            try:
                spec = factory.load_spec(run_id=run_id)
            except Exception as e:
                _registry.mark_error(run_id=run_id, error_type=type(e).__name__, error_message=str(e))
                raise HTTPException(status_code=409, detail=f"failed to load RunSpec: {e}")

            try:
                handle = factory.build(run_id=run_id, spec=spec)
            except Exception as e:
                _registry.mark_error(run_id=run_id, error_type=type(e).__name__, error_message=str(e))
                raise HTTPException(status_code=409, detail=f"failed to build run: {e}")

            _live[run_id] = handle

    try:
        handle.lifecycle.start()
        _registry.mark_running(run_id=run_id)
        return StartRunResponse(run_id=run_id, status="running")
    except Exception as e:
        _registry.mark_error(run_id=run_id, error_type=type(e).__name__, error_message=str(e))
        # If start fails, don't keep a broken live handle around
        with _live_lock:
            _live.pop(run_id, None)
        raise HTTPException(status_code=409, detail=str(e))


@router.post("/runs/{run_id}/stop", response_model=StopRunResponse)
def stop_run(run_id: str) -> StopRunResponse:
    art = artifacts_for(runs_dir=settings.runs_dir, run_id=run_id)
    if not art.run_dir.exists():
        raise HTTPException(status_code=404, detail="run not found")

    with _live_lock:
        handle = _live.get(run_id)

    if handle is None:
        raise HTTPException(status_code=409, detail="run is not running in this process")

    try:
        handle.lifecycle.stop()
        _registry.mark_stopped(run_id=run_id)

        # remove handle so a future /start creates a fresh lifecycle
        with _live_lock:
            _live.pop(run_id, None)

        return StopRunResponse(run_id=run_id, status="stopped")
    except Exception as e:
        _registry.mark_error(run_id=run_id, error_type=type(e).__name__, error_message=str(e))
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/runs", response_model=RunsListResponse)
def list_runs() -> RunsListResponse:
    out: list[RunDetailsResponse] = []
    for rec in _registry.list():
        art = artifacts_for(runs_dir=settings.runs_dir, run_id=rec.run_id)
        out.append(
            RunDetailsResponse(
                run_id=rec.run_id,
                status=rec.status,
                created_at_utc=rec.created_at_utc,
                updated_at_utc=rec.updated_at_utc,
                run_dir=str(art.run_dir),
                artifacts={
                    "config_json": str(art.config_json),
                    "meta_json": str(art.meta_json),
                    "events_jsonl": str(art.events_jsonl),
                    "metrics_json": str(art.metrics_json),
                    "evaluation_json": str(art.evaluation_json),
                    "engine_log": str(art.engine_log),
                },
                error_type=rec.error_type,
                error_message=rec.error_message,
            )
        )
    return RunsListResponse(runs=out)


@router.get("/runs/{run_id}", response_model=RunDetailsResponse)
def get_run(run_id: str) -> RunDetailsResponse:
    rec = _registry.get(run_id=run_id)
    art = artifacts_for(runs_dir=settings.runs_dir, run_id=run_id)

    if not art.run_dir.exists():
        raise HTTPException(status_code=404, detail="run not found")

    if rec is None:
        created = datetime.fromtimestamp(art.run_dir.stat().st_mtime)
        updated = created
        status: RunStatus = "created"
        error_type = None
        error_message = None
    else:
        created = rec.created_at_utc
        updated = rec.updated_at_utc
        status = rec.status
        error_type = rec.error_type
        error_message = rec.error_message

    return RunDetailsResponse(
        run_id=run_id,
        status=status,
        created_at_utc=created,
        updated_at_utc=updated,
        run_dir=str(art.run_dir),
        artifacts={
            "config_json": str(art.config_json),
            "meta_json": str(art.meta_json),
            "events_jsonl": str(art.events_jsonl),
            "metrics_json": str(art.metrics_json),
            "evaluation_json": str(art.evaluation_json),
            "engine_log": str(art.engine_log),
        },
        error_type=error_type,
        error_message=error_message,
    )
