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

from shared.config import DATA_DIR, GN_DATA_DIR, OA_DATA_DIR, OSM_URL
from shared.logging import get_logger
from shared.valhalla import link_pbf_for_valhalla

logger = get_logger("downloader")

# Set SSL_VERIFY=false in .env to disable certificate verification (dev only)
_SSL_VERIFY = os.getenv("SSL_VERIFY", "true").lower() not in ("false", "0", "no")

_OA_URL = os.getenv("OA_URL", "")
_GN_URL = os.getenv("GN_URL", "")


def _download_file(url: str, dest_dir: str, label: str = "downloader"):
    """Download a single file into *dest_dir*, skipping if it already exists."""
    os.makedirs(dest_dir, exist_ok=True)

    filename = url.rsplit("/", 1)[-1]
    filepath = os.path.join(dest_dir, filename)

    if os.path.exists(filepath):
        logger.info(f"[{label}] {filename} already exists, skipping")
        return filepath

    logger.info(f"[{label}] Downloading {filename} ...")
    resp = requests.get(url, stream=True, timeout=60, verify=_SSL_VERIFY)
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
                    f"\r[{label}] {downloaded * 100 // total}%  ({downloaded}/{total} bytes)",
                    end="",
                )

    os.rename(tmp, filepath)
    logger.info(f"\n[{label}] Saved {filename}")
    return filepath


def download():
    # ── OSM PBF ───────────────────────────────────────────────────────────
    if OSM_URL:
        pbf_path = _download_file(OSM_URL, DATA_DIR, label="downloader")
        link_pbf_for_valhalla(pbf_path, label="downloader")
    else:
        logger.warning("[downloader] No osm_url configured, skipping OSM download")

    # ── OpenAddresses ─────────────────────────────────────────────────────
    if _OA_URL:
        _download_file(_OA_URL, OA_DATA_DIR, label="downloader/oa")
    else:
        logger.warning("[downloader] No OA_URL configured, skipping OpenAddresses download")

    # ── GeoNames ──────────────────────────────────────────────────────────
    if _GN_URL:
        _download_file(_GN_URL, GN_DATA_DIR, label="downloader/gn")
    else:
        logger.warning("[downloader] No GN_URL configured, skipping GeoNames download")


if __name__ == "__main__":
    download()
