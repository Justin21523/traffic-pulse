from __future__ import annotations

import logging.config
import os
from pathlib import Path
from typing import Any

import yaml

from trafficpulse.settings import project_root


def configure_logging(logging_config_path: str | Path | None = None) -> None:
    root = project_root()
    candidate = logging_config_path or os.getenv(
        "TRAFFICPULSE_LOGGING_CONFIG", "configs/logging.yaml"
    )
    path = Path(candidate)
    if not path.is_absolute():
        path = root / path
    if not path.exists():
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "standard": {
                        "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
                    }
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "level": "INFO",
                        "formatter": "standard",
                        "stream": "ext://sys.stderr",
                    }
                },
                "root": {"level": "INFO", "handlers": ["console"]},
            }
        )
        return

    config: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    logging.config.dictConfig(config)

