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


def is_connection_error(err: Exception) -> bool:
    """Check if an error indicates the NATS connection is broken and needs reconnection."""
    error_str = str(err).lower()
    return any(
        keyword in error_str 
        for keyword in ["serviceunavailable", "connection", "disconnect", "closed", "broken"]
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
    
    # Desired stream config — applied whether the stream is new or already exists.
    # Calling update_stream on an existing stream is idempotent: unchanged fields
    # are left alone, so re-deploying with a new max_msg_size takes effect
    # immediately without wiping stored messages.
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

    max_retries = 5
    for attempt in range(max_retries):
        try:
            await js.find_stream_name_by_subject(NATS_SUBJECT)
            # Stream exists — always update to pick up any config changes
            # (e.g. max_msg_size bumped from 1 MB to unlimited after a redeploy).
            try:
                await js.update_stream(_STREAM_CFG)
                print("[nats_client] Stream config updated", flush=True)
            except Exception as upd_err:
                print(f"[nats_client] Stream update skipped: {upd_err}", flush=True)
            return nc, js
        except Exception as e:
            is_transient = is_transient_error(e)
            if is_transient and attempt < max_retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
                continue

            try:
                await js.add_stream(_STREAM_CFG)
                print("[nats_client] Stream created", flush=True)
                return nc, js
            except Exception as e2:
                is_transient2 = is_transient_error(e2)
                if (is_transient2 or "already exists" in str(e2).lower()) and attempt < max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
                elif attempt >= max_retries - 1:
                    raise

    return nc, js


async def reconnect(nc, js):
    """Reconnect to NATS and return new (nc, js). Close old connection if it exists."""
    import asyncio
    
    print("[nats_client] Attempting to reconnect to NATS...", flush=True)
    
    # Close old connection if it exists
    if nc and not nc.is_closed:
        try:
            await nc.close()
            print("[nats_client] Closed old NATS connection", flush=True)
        except Exception as e:
            print(f"[nats_client] Error closing old connection: {e}", flush=True)
    
    # Create new connection
    return await connect()


async def subscribe(js, consumer_name):
    """Create a durable pull subscription for a consumer with retry logic for transient errors."""
    import asyncio
    from nats.js.api import ConsumerConfig
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Try to get existing consumer info first
            await js.consumer_info(NATS_STREAM, consumer_name)
            # Consumer exists, create subscription to it
            print(f"[nats_client] Using existing consumer: {consumer_name}", flush=True)
            return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
        except Exception as e:
            is_transient = is_transient_error(e)
            print(f"[nats_client] Consumer {consumer_name} not found (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
            
            # Consumer doesn't exist, create it with proper config
            try:
                print(f"[nats_client] Creating new consumer: {consumer_name}", flush=True)
                return await js.pull_subscribe(
                    NATS_SUBJECT, 
                    durable=consumer_name, 
                    stream=NATS_STREAM,
                    config=ConsumerConfig(
                        max_waiting=10,
                        max_deliver=5,
                        ack_wait=120,
                    )
                )
            except Exception as e2:
                is_transient2 = is_transient_error(e2)
                print(f"[nats_client] Failed to create consumer with config (attempt {attempt + 1}/{max_retries}): {e2}", flush=True)
                
                # Fallback to simple subscription
                try:
                    print(f"[nats_client] Trying simple subscription for: {consumer_name}", flush=True)
                    return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
                except Exception as e3:
                    is_transient3 = is_transient_error(e3)
                    
                    if (is_transient or is_transient2 or is_transient3) and attempt < max_retries - 1:
                        print(f"[nats_client] Retrying subscription creation (attempt {attempt + 1}/{max_retries})", flush=True)
                        await asyncio.sleep(2 * (attempt + 1))  # Increased backoff
                        continue
                    else:
                        print(f"[nats_client] Failed to create subscription after {max_retries} attempts: {e3}", flush=True)
                        raise
