"""Loader for the `spec/` directory, billing's copy.

Deliberately duplicated from `shared/spec.py` rather than imported: AD-13 keeps
this subsystem independently deployable, so `billing/` imports nothing from
`shared/`. Same precedent as the Redis client. The *data* in `spec/` is shared;
the loader is not.

`spec/billing.toml` is specification, not configuration — plan pricing, per
endpoint credit weights, metering units, security minimums. Those decide what
customers are charged, so a regenerated implementation must read them rather
than invent a price list.

`spec/` ships in the image (see billing/Dockerfile); it is a runtime input.
"""

from __future__ import annotations

import tomllib
from functools import cache
from pathlib import Path
from typing import Any

SPEC_DIR = Path(__file__).resolve().parent.parent / "spec"


@cache
def load(filename: str = "billing.toml") -> dict[str, Any]:
    path = SPEC_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"missing specification file {path}. spec/ is a runtime input; check the "
            "image COPYs it (billing/Dockerfile)."
        )
    with path.open("rb") as fh:
        return tomllib.load(fh)
