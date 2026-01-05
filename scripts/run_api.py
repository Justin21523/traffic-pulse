from __future__ import annotations

import uvicorn

from trafficpulse.settings import get_config


def main() -> None:
    config = get_config()
    uvicorn.run(
        "trafficpulse.api.app:app",
        host=config.api.host,
        port=int(config.api.port),
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()

