from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """
    Global application-level configuration.

    This class is the single source of truth for:
    - environment selection
    - logging behavior
    - reproducibility defaults
    - run artifact locations
    """

    model_config = SettingsConfigDict(
        env_prefix="MMRL_",
        env_file=".env",
        extra="ignore",
    )

    # ---- Environment -------------------------------------------------

    env: Literal["local", "staging", "prod"] = "local"

    # ---- Logging -----------------------------------------------------

    log_level: str = "INFO"

    # ---- Runs & Reproducibility -------------------------------------

    # Root directory where all run artifacts are stored
    runs_dir: Path = Field(
        default=Path("runs"),
        description="Root directory for run artifacts",
    )

    # Default seed (can be overridden per run)
    default_seed: int = Field(
        default=42,
        description="Default RNG seed for reproducibility",
    )


# Singleton settings object
settings = AppSettings()
