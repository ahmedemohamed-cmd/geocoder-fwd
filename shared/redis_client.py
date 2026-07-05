"""Redis client factory — standalone (default) or Redis Cluster.

Every service builds its client here so REDIS_MODE is applied uniformly:

    REDIS_MODE=standalone   redis.Redis / redis.asyncio.Redis at host:port
    REDIS_MODE=cluster      RedisCluster bootstrapped from REDIS_NODES
                            ("host1:6379,host2:6379"; empty falls back to
                            host:port as the sole startup node)

Cluster-mode constraints our call sites respect:

* no logical databases — a cluster has no SELECT, so ``db`` is dropped here
  and key isolation must come from key prefixes;
* Lua scripts must keep all KEYS in one hash slot (see traffic_aggregator's
  single-key EWMA script);
* multi-key commands are only safe when the client can split them per slot,
  so bulk deletes go through per-key pipelines;
* pipelines are non-transactional (``transaction=False``), which is what the
  cluster clients provide.
"""

from __future__ import annotations

import logging

import redis as redis_sync
import redis.asyncio as redis_async

from shared.config import REDIS_HOST, REDIS_MODE, REDIS_NODES, REDIS_PORT

logger = logging.getLogger(__name__)


def _parse_nodes(default_host: str, default_port: int) -> list[tuple[str, int]]:
    nodes: list[tuple[str, int]] = []
    for item in REDIS_NODES.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            host, port_s = item.rsplit(":", 1)
            nodes.append((host, int(port_s)))
        else:
            nodes.append((item, 6379))
    return nodes or [(default_host, default_port)]


def make_redis(host: str = REDIS_HOST, port: int = REDIS_PORT, **kwargs):
    """Synchronous client honoring REDIS_MODE (``db`` is dropped in cluster mode)."""
    if REDIS_MODE == "cluster":
        from redis.cluster import ClusterNode, RedisCluster

        if kwargs.pop("db", 0):
            logger.warning("Redis cluster mode has no logical DBs; 'db' ignored")
        startup = [ClusterNode(h, p) for h, p in _parse_nodes(host, port)]
        return RedisCluster(startup_nodes=startup, **kwargs)
    return redis_sync.Redis(host=host, port=port, **kwargs)


def make_redis_async(host: str = REDIS_HOST, port: int = REDIS_PORT, **kwargs):
    """Asyncio client honoring REDIS_MODE (``db`` is dropped in cluster mode)."""
    if REDIS_MODE == "cluster":
        from redis.asyncio.cluster import ClusterNode, RedisCluster

        if kwargs.pop("db", 0):
            logger.warning("Redis cluster mode has no logical DBs; 'db' ignored")
        startup = [ClusterNode(h, p) for h, p in _parse_nodes(host, port)]
        return RedisCluster(startup_nodes=startup, **kwargs)
    return redis_async.Redis(host=host, port=port, **kwargs)
