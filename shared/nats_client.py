import nats
from nats.js.api import StreamConfig, RetentionPolicy
from shared.config import NATS_URL, NATS_STREAM, NATS_SUBJECT


def is_transient_error(err: Exception) -> bool:
    """Check if an error is a transient NATS error that should be retried."""
    error_str = str(err).lower()
    return any(
        keyword in error_str 
        for keyword in ["serviceunavailable", "timeout", "connection", "disconnect"]
    )


async def connect():
    """Connect to NATS and return (nc, js) with the OSM stream ensured."""
    import asyncio
    
    # Retry connection with exponential backoff
    max_retries = 10
    base_retry_delay = 2
    
    nc = None
    for attempt in range(max_retries):
        try:
            nc = await nats.connect(NATS_URL)
            js = nc.jetstream()
            break
        except Exception as e:
            is_transient = is_transient_error(e)
            print(f"[nats_client] Failed to connect to NATS (attempt {attempt + 1}/{max_retries}): {e} (transient: {is_transient})", flush=True)
            
            if attempt < max_retries - 1:
                # Use exponential backoff for transient errors
                if is_transient:
                    delay = base_retry_delay * (2 ** attempt)  # Exponential backoff
                else:
                    delay = base_retry_delay
                await asyncio.sleep(min(delay, 30))  # Cap at 30 seconds
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
        except Exception as e:
            is_transient = is_transient_error(e)
            if is_transient and attempt < max_retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
                continue
            
            try:
                await js.add_stream(
                    StreamConfig(
                        name=NATS_STREAM,
                        subjects=[NATS_SUBJECT],
                        retention=RetentionPolicy.LIMITS,  # Keep messages until limits are reached
                        max_age=86400,  # Keep messages for 24 hours (in seconds)
                        max_bytes=10737418240,  # 10GB max storage
                        storage="file",  # Store data on disk
                        max_msg_size=1048576,  # 1MB max message size
                        discard="old",  # Discard old messages when limits are reached
                    )
                )
                return nc, js
            except Exception as e2:
                is_transient2 = is_transient_error(e2)
                if (is_transient2 or "already exists" in str(e2).lower()) and attempt < max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
                elif attempt >= max_retries - 1:
                    raise
    
    return nc, js


async def subscribe(js, consumer_name):
    """Create a durable pull subscription for a consumer with retry logic for transient errors."""
    import asyncio
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Try to get existing consumer info first
            await js.consumer_info(NATS_STREAM, consumer_name)
            # Consumer exists, use it
            return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
        except Exception as e:
            is_transient = is_transient_error(e)
            
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
            except Exception as e2:
                is_transient2 = is_transient_error(e2)
                
                # Fallback to simple subscription
                try:
                    return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
                except Exception as e3:
                    is_transient3 = is_transient_error(e3)
                    
                    if (is_transient or is_transient2 or is_transient3) and attempt < max_retries - 1:
                        print(f"[nats_client] Retrying subscription creation (attempt {attempt + 1}/{max_retries})", flush=True)
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    else:
                        print(f"[nats_client] Failed to create subscription after {max_retries} attempts: {e3}", flush=True)
                        raise
