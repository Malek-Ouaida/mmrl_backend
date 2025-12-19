from __future__ import annotations

import structlog
from fastapi import FastAPI

from mmrl.api import router as api_router
from mmrl.core.config.settings import settings
from mmrl.core.logging.setup import configure_logging

log = structlog.get_logger()


def create_app() -> FastAPI:
    """
    Application factory.

    This function is the single place where the FastAPI app
    is created and configured.
    """
    # Initialize structured logging
    configure_logging(level=settings.log_level)

    app = FastAPI(
        title="MMRL Backend",
        version="0.1.0",
    )

    @app.on_event("startup")
    async def on_startup() -> None:
        log.info(
            "app.startup",
            environment=settings.env,
        )

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        log.info("app.shutdown")

    # Mount API
    app.include_router(api_router, prefix="/api")

    return app


# ASGI entrypoint
app = create_app()
