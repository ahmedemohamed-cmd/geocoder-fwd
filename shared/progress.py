"""Reusable progress tracker for data-import services."""

import time

from shared.logging import get_logger

logger = get_logger("progress")


class ProgressTracker:
    """Simple progress tracker that logs updates at regular intervals."""

    def __init__(self, description: str, total: int = 0, log_interval: int = 5):
        self.description = description
        self.total = total
        self.log_interval = log_interval
        self.count = 0
        self.skipped = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time

    def update(self, n: int = 1):
        self.count += n
        now = time.time()
        if now - self.last_log_time >= self.log_interval:
            self._log()
            self.last_log_time = now

    def skip(self, n: int = 1):
        self.skipped += n

    def _log(self):
        elapsed = time.time() - self.start_time
        processed = self.count + self.skipped
        rate = processed / elapsed if elapsed > 0 else 0
        if self.total > 0:
            pct = processed / self.total * 100
            logger.info(
                "[%s] %s/%s (%s%%) - %s items/sec  (%s published, %s skipped)",
                self.description,
                processed,
                self.total,
                format(pct, ".1f"),
                format(rate, ".0f"),
                self.count,
                self.skipped,
            )
        else:
            logger.info(
                "[%s] %s published - %s items/sec  (%s skipped)",
                self.description,
                self.count,
                format(rate, ".0f"),
                self.skipped,
            )

    def close(self):
        self._log()
        elapsed = time.time() - self.start_time
        logger.info(
            "[%s] Completed in %ss -- %s published, %s skipped",
            self.description,
            format(elapsed, ".1f"),
            self.count,
            self.skipped,
        )
