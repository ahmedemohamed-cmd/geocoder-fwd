"""Download OSM PBF, OpenAddresses, and GeoNames files into data/.

Environment variables
---------------------
osm_url       URL of an OSM PBF extract (e.g. Geofabrik)
OA_URL        URL of an OpenAddresses CSV or GeoJSON archive
GN_URL        URL of a GeoNames dump zip (e.g. allCountries.zip or CA.zip)
SSL_VERIFY    Set to ``false`` to skip certificate checks (dev only)
"""

import os

import requests

from shared.config import (
    DATA_DIR,
    GN_DATA_DIR,
    GN_URL,
    OA_DATA_DIR,
    OA_URL,
    OSM_URL,
    SSL_VERIFY,
)
from shared.logging import get_logger
from shared.valhalla import link_pbf_for_valhalla

logger = get_logger("downloader")


def _download_file(url: str, dest_dir: str, label: str = "downloader"):
    """Download a single file into *dest_dir*, skipping if it already exists."""
    os.makedirs(dest_dir, exist_ok=True)

    filename = url.rsplit("/", 1)[-1]
    filepath = os.path.join(dest_dir, filename)

    if os.path.exists(filepath):
        logger.info("[%s] %s already exists, skipping", label, filename)
        return filepath

    logger.info("[%s] Downloading %s ...", label, filename)
    resp = requests.get(url, stream=True, timeout=60, verify=SSL_VERIFY)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    tmp = filepath + ".tmp"

    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 16):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                logger.info(
                    "\r[%s] %s%%  (%s/%s bytes)",
                    label,
                    downloaded * 100 // total,
                    downloaded,
                    total,
                    end="",
                )

    os.rename(tmp, filepath)
    logger.info("\n[%s] Saved %s", label, filename)
    return filepath


def download():
    # ── OSM PBF ───────────────────────────────────────────────────────────
    if OSM_URL:
        pbf_path = _download_file(OSM_URL, DATA_DIR, label="downloader")
        link_pbf_for_valhalla(pbf_path, label="downloader")
    else:
        logger.warning("[downloader] No osm_url configured, skipping OSM download")

    # ── OpenAddresses ─────────────────────────────────────────────────────
    if OA_URL:
        _download_file(OA_URL, OA_DATA_DIR, label="downloader/oa")
    else:
        logger.warning("[downloader] No OA_URL configured, skipping OpenAddresses download")

    # ── GeoNames ──────────────────────────────────────────────────────────
    if GN_URL:
        _download_file(GN_URL, GN_DATA_DIR, label="downloader/gn")
    else:
        logger.warning("[downloader] No GN_URL configured, skipping GeoNames download")


if __name__ == "__main__":
    download()
