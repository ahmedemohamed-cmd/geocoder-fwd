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
                f"[{self.description}] {processed}/{self.total} ({pct:.1f}%) "
                f"- {rate:.0f} items/sec  ({self.count} published, {self.skipped} skipped)",
            )
        else:
            logger.info(
                f"[{self.description}] {self.count} published - {rate:.0f} items/sec"
                f"  ({self.skipped} skipped)",
            )

    def close(self):
        self._log()
        elapsed = time.time() - self.start_time
        logger.info(
            f"[{self.description}] Completed in {elapsed:.1f}s -- "
            f"{self.count} published, {self.skipped} skipped",
        )
