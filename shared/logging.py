"""Small, consistent logging setup shared by every entrypoint.

Services historically used ``print(f"[service] ...", flush=True)`` for
diagnostics, which gives no levels, no filtering and inconsistent formatting in
container logs. ``get_logger`` replaces that with the stdlib ``logging`` module,
configured once from the ``LOG_LEVEL`` environment variable.

Usage::

    from shared.logging import get_logger
    logger = get_logger("watcher")          # -> logger name "geocoder.watcher"
    logger.info("Processed %d nodes", count)
    logger.error("Error caching node %s: %s", node_id, exc)
"""

import logging
import sys

from shared.config import LOG_LEVEL

_CONFIGURED = False


def _configure_root() -> None:
    """Attach a single stdout handler to the ``geocoder`` logger tree once.

    Idempotent: safe to call from every module at import time. Honors
    ``LOG_LEVEL`` (default INFO); stdout is line-buffered/auto-flushed per
    record, so the old ``flush=True`` on prints is no longer needed.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    level = getattr(logging, LOG_LEVEL, logging.INFO)

    root = logging.getLogger("geocoder")
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
        root.addHandler(handler)
    # Don't double-emit through the Python root logger's default handler.
    root.propagate = False
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a child of the shared ``geocoder`` logger.

    ``name`` is a short service/module identifier (e.g. ``"watcher"``); the
    returned logger is ``geocoder.<name>``.
    """
    _configure_root()
    return logging.getLogger(f"geocoder.{name}")
