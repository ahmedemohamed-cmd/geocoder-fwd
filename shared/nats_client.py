import nats
from nats.js.api import StreamConfig, RetentionPolicy
from shared.config import NATS_URL, NATS_STREAM, NATS_SUBJECT


async def connect():
    """Connect to NATS and return (nc, js) with the OSM stream ensured."""
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    
    # Retry stream creation with backoff
    max_retries = 5
    for attempt in range(max_retries):
        try:
            await js.find_stream_name_by_subject(NATS_SUBJECT)
            return nc, js
        except Exception:
            try:
                await js.add_stream(
                    StreamConfig(
                        name=NATS_STREAM,
                        subjects=[NATS_SUBJECT],
                        retention=RetentionPolicy.LIMITS,  # Keep messages until limits are reached
                        max_age=0,  # Keep messages indefinitely (0 = unlimited)
                        max_bytes=-1,  # No size limit
                        storage="file",  # Store data on disk
                    )
                )
                return nc, js
            except Exception as e:
                if attempt < max_retries - 1:
                    import asyncio
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    raise
    
    return nc, js


async def subscribe(js, consumer_name):
    """Create a durable pull subscription for a consumer."""
    return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
