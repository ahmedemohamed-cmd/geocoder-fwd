"""Download OSM PBF files listed in .env into data/ if not already present."""

import os

import requests

from shared.config import OSM_URL, DATA_DIR

# Set SSL_VERIFY=false in .env to disable certificate verification (dev only)
_SSL_VERIFY = os.getenv("SSL_VERIFY", "true").lower() not in ("false", "0", "no")


def download():
    os.makedirs(DATA_DIR, exist_ok=True)

    if not OSM_URL:
        print("[downloader] No osm_url configured in .env, nothing to download")
        return

    filename = OSM_URL.rsplit("/", 1)[-1]
    filepath = os.path.join(DATA_DIR, filename)

    if os.path.exists(filepath):
        print(f"[downloader] {filename} already exists, skipping")
        return

    print(f"[downloader] Downloading {filename} ...")
    resp = requests.get(OSM_URL, stream=True, timeout=60, verify=_SSL_VERIFY)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    tmp = filepath + ".tmp"

    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 16):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                print(
                    f"\r[downloader] {downloaded * 100 // total}%"
                    f"  ({downloaded}/{total} bytes)",
                    end="",
                    flush=True,
                )

    os.rename(tmp, filepath)
    print(f"\n[downloader] Saved {filename}")


if __name__ == "__main__":
    download()
