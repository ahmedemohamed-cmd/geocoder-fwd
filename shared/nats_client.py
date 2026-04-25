import nats
from nats.js.api import StreamConfig, RetentionPolicy
from shared.config import NATS_URL, NATS_STREAM, NATS_SUBJECT


async def connect():
    """Connect to NATS and return (nc, js) with the OSM stream ensured."""
    import asyncio
    
    # Retry connection with backoff
    max_retries = 10
    retry_delay = 2
    
    nc = None
    for attempt in range(max_retries):
        try:
            nc = await nats.connect(NATS_URL)
            js = nc.jetstream()
            break
        except Exception as e:
            print(f"[nats_client] Failed to connect to NATS (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise
    
    if nc is None:
        raise Exception("Failed to connect to NATS after maximum retries")
    
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
    try:
        # Try to get existing consumer info first
        await js.consumer_info(NATS_STREAM, consumer_name)
        # Consumer exists, use it
        return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
    except Exception:
        # Consumer doesn't exist, create it with proper config
        try:
            from nats.js.api import ConsumerConfig
            return await js.pull_subscribe(
                NATS_SUBJECT, 
                durable=consumer_name, 
                stream=NATS_STREAM,
                config=ConsumerConfig(
                    max_waiting=10,
                    max_deliver=1,
                    ack_wait=30,
                )
            )
        except Exception:
            # Fallback to simple subscription
            return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
