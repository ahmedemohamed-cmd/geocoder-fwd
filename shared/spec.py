"""Loader for the `spec/` directory.

`spec/` holds specification, not configuration. The difference matters: a
configuration value is an operator's choice for one deployment, while these are
tuning decisions that define what the system *is* — ranking weights, category
taxonomies, address-parsing vocabularies, the Elasticsearch analyzer chain.
None of them are derivable from the architecture, so they live in declarative
files that an implementation reads rather than embeds.

Consequences worth knowing:

* Changing a spec file changes behaviour. Measure against
  docs/quality-baseline.md — see AD-9 in the geocoder architecture spine.
* `spec/` ships in the container image (see Dockerfile); it is a runtime input,
  not documentation.
* Loads are cached per process: these files are read at import time and never
  reload, so a change requires a restart.
"""

from __future__ import annotations

import json
import tomllib
from functools import cache
from pathlib import Path
from typing import Any

SPEC_DIR = Path(__file__).resolve().parent.parent / "spec"


@cache
def load(filename: str) -> dict[str, Any]:
    """Return the parsed spec document, by filename (e.g. ``ranking.toml``)."""
    path = SPEC_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"missing specification file {path}. spec/ is a runtime input; "
            "check it is present and, in a container, that the image COPYs it."
        )
    if path.suffix == ".toml":
        with path.open("rb") as fh:
            return tomllib.load(fh)
    if path.suffix == ".json":
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    raise ValueError(f"unsupported spec format: {path.suffix}")
