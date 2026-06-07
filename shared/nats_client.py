import asyncio
import logging

import nats
from nats.js.api import ConsumerConfig, StreamConfig, RetentionPolicy
from shared.config import NATS_URL, NATS_STREAM, NATS_SUBJECT

logger = logging.getLogger(__name__)

# Desired stream config — applied whether the stream is new or already exists.
_STREAM_CFG = StreamConfig(
    name=NATS_STREAM,
    subjects=[NATS_SUBJECT],
    retention=RetentionPolicy.LIMITS,
    max_age=86400,           # 24 h
    max_bytes=10737418240,   # 10 GB
    storage="file",
    max_msg_size=-1,         # unlimited — server ceiling is 64 MB (nats.conf)
    discard="old",
)


def is_transient_error(err: Exception) -> bool:
    """Check if an error is a transient NATS error that should be retried."""
    error_str = str(err).lower()
    return any(
        keyword in error_str
        for keyword in ["serviceunavailable", "timeout", "connection", "disconnect"]
    )


def is_connection_error(err: Exception) -> bool:
    """Check if an error indicates the NATS connection is broken and needs reconnection."""
    error_str = str(err).lower()
    return any(
        keyword in error_str
        for keyword in ["serviceunavailable", "connection", "disconnect", "closed", "broken"]
    )


async def connect():
    """Connect to NATS and return (nc, js) with the OSM stream ensured."""
    max_retries = 10
    base_retry_delay = 2

    nc = None
    for attempt in range(max_retries):
        try:
            nc = await nats.connect(NATS_URL)
            js = nc.jetstream()
            break
        except Exception as e:
            transient = is_transient_error(e)
            logger.warning(
                "Failed to connect to NATS (attempt %d/%d): %s (transient: %s)",
                attempt + 1, max_retries, e, transient,
            )
            if attempt < max_retries - 1:
                delay = base_retry_delay * (2 ** attempt) if transient else base_retry_delay
                await asyncio.sleep(min(delay, 30))
            else:
                raise

    if nc is None:
        raise RuntimeError("Failed to connect to NATS after maximum retries")

    # Ensure stream exists and config is up-to-date
    stream_retries = 5
    for attempt in range(stream_retries):
        try:
            await js.find_stream_name_by_subject(NATS_SUBJECT)
            # Stream exists — update to pick up config changes
            try:
                await js.update_stream(_STREAM_CFG)
                logger.info("Stream config updated")
            except Exception as upd_err:
                logger.debug("Stream update skipped: %s", upd_err)
            return nc, js
        except Exception as e:
            if is_transient_error(e) and attempt < stream_retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
                continue
            # Stream doesn't exist — create it
            try:
                await js.add_stream(_STREAM_CFG)
                logger.info("Stream created")
                return nc, js
            except Exception as e2:
                if (is_transient_error(e2) or "already exists" in str(e2).lower()) and attempt < stream_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
                elif attempt >= stream_retries - 1:
                    raise

    return nc, js


async def reconnect(nc, js):
    """Reconnect to NATS and return new (nc, js). Close old connection if it exists."""
    logger.info("Attempting to reconnect to NATS...")

    if nc and not nc.is_closed:
        try:
            await nc.close()
            logger.info("Closed old NATS connection")
        except Exception as e:
            logger.warning("Error closing old connection: %s", e)

    return await connect()


async def subscribe(js, consumer_name):
    """Create a durable pull subscription for a consumer with retry logic.

    Always attempts to attach with the desired ConsumerConfig so that
    max_deliver / ack_wait are applied even if the consumer already exists
    (NATS 2.10+ updates mutable fields; earlier versions accept the attach
    when config is compatible).  Falls back to a config-free attach with a
    warning if the server rejects the config — this can happen when the
    consumer exists with immutable fields that differ from the desired config.

    After obtaining the subscription object we verify with ``consumer_info``
    that the server-side consumer really exists.
    """
    max_retries = 5
    for attempt in range(max_retries):
        sub = None
        try:
            # Always pass ConsumerConfig so the server creates or updates
            # max_deliver / ack_wait regardless of whether the consumer exists.
            sub = await js.pull_subscribe(
                NATS_SUBJECT,
                durable=consumer_name,
                stream=NATS_STREAM,
                config=ConsumerConfig(
                    max_waiting=10,
                    max_deliver=-1,   # unlimited redelivery
                    ack_wait=300,     # 5 min to allow slow bulk inserts
                ),
            )
            logger.info("Attached to consumer %s with desired config", consumer_name)
        except Exception as e:
            logger.debug(
                "ConsumerConfig attach failed for %s (attempt %d/%d): %s — trying simple attach",
                consumer_name, attempt + 1, max_retries, e,
            )
            # Fall back to a config-free attach (works when consumer already
            # exists with incompatible immutable fields).  Log a warning so the
            # operator knows the consumer config may be stale.
            try:
                sub = await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
                logger.warning(
                    "Consumer %s: attached without config update — existing consumer "
                    "config may have stale max_deliver/ack_wait. "
                    "Delete the consumer to force recreation with new settings.",
                    consumer_name,
                )
            except Exception as e2:
                any_transient = is_transient_error(e) or is_transient_error(e2)
                if any_transient and attempt < max_retries - 1:
                    logger.warning("Retrying subscription creation (attempt %d/%d)", attempt + 1, max_retries)
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                logger.error("Failed to create subscription after %d attempts: %s", max_retries, e2)
                raise

        # Verify the consumer actually exists on the server
        if sub is not None:
            try:
                await js.consumer_info(NATS_STREAM, consumer_name)
                logger.info("Verified consumer %s exists on server", consumer_name)
                return sub
            except Exception as verify_err:
                logger.warning(
                    "Consumer %s not found on server after subscribe (attempt %d/%d): %s",
                    consumer_name, attempt + 1, max_retries, verify_err,
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                raise RuntimeError(
                    f"Consumer {consumer_name} was not created on the server"
                ) from verify_err

    raise RuntimeError(f"Failed to subscribe consumer {consumer_name} after {max_retries} attempts")
