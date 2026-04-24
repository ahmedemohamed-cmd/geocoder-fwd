import nats
from nats.js.api import StreamConfig
from shared.config import NATS_URL, NATS_STREAM, NATS_SUBJECT


async def connect():
    """Connect to NATS and return (nc, js) with the OSM stream ensured."""
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        await js.find_stream_name_by_subject(NATS_SUBJECT)
    except Exception:
        await js.add_stream(
            StreamConfig(
                name=NATS_STREAM,
                subjects=[NATS_SUBJECT],
                retention="limits",
                max_age=0,  # Keep messages indefinitely (0 = unlimited)
                max_bytes=-1,  # No size limit
            )
        )
    return nc, js


async def subscribe(js, consumer_name):
    """Create a durable pull subscription for a consumer."""
    return await js.pull_subscribe(NATS_SUBJECT, durable=consumer_name, stream=NATS_STREAM)
