"""Expose OSM extracts at the top of data/ to the Valhalla container.

Valhalla runs with path_extension=valhalla and only globs
/custom_files/valhalla/*.pbf, while the extracts themselves live at the top of
data/ for the watcher and friends. Relative symlinks in data/valhalla/ bridge
the two; they resolve both on the host and inside any container that mounts
data/.
"""

import os

from shared.logging import get_logger

logger = get_logger("valhalla")


def link_pbf_for_valhalla(filepath: str, label: str = "valhalla"):
    """Symlink a PBF into <its dir>/valhalla/ so the routing engine picks it up.

    Idempotent: a correct existing link is left untouched, a stale one is
    replaced, and a real file with the same name is never clobbered.
    """
    valhalla_dir = os.path.join(os.path.dirname(filepath), "valhalla")
    os.makedirs(valhalla_dir, exist_ok=True)

    filename = os.path.basename(filepath)
    link = os.path.join(valhalla_dir, filename)
    target = os.path.join("..", filename)

    if os.path.islink(link):
        if os.readlink(link) == target:
            return
        os.remove(link)
    elif os.path.exists(link):
        logger.info(f"[{label}] {link} exists and is not a symlink, leaving it")
        return
    os.symlink(target, link)
    logger.info(f"[{label}] Linked {link} -> {target} for Valhalla")
