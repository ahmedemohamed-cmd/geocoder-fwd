"""The rule that makes tests/acceptance/ survivable.

This suite is the regeneration contract: it must pass against a *different*
implementation of the same specification. That only holds if it never reaches
into production internals — a test calling `routing._translate_maneuver` fails
with AttributeError against any implementation that organised itself
differently, and worse, forces a regeneration to reproduce our private names.

Two failure modes this guards against:

1. A regeneration is judged non-compliant for cosmetic reasons.
2. A regeneration is *told* to satisfy internal-shaped tests, which smuggles in
   knowledge the specs lack — so the specs look sufficient while the gap hides.

Implementation-scoped tests belong in tests/unit/, which is expected to be
rewritten alongside the code.
"""

import ast
import pathlib

import pytest

ACCEPTANCE = pathlib.Path(__file__).parent
PRODUCTION = {"services", "shared", "billing"}

# Module-level API this project has declared contractual: a regeneration must
# keep these import paths and signatures, and is free inside them.
CONTRACTUAL_MODULES = {
    "shared.categories",
    "shared.address",
    "shared.ranking",
    "shared.autocomplete",
    "shared.places_mapping",
    "shared.es_mapping",
    "shared.spec",
    "shared.config",
    "shared.nats_client",
    "shared.processed",
    "billing.billing_engine",
    "billing.weights",
    "billing.config",
    "billing.usage",
    "billing.db",
    "billing.spec",
    "billing.repo",
    "billing.security",
    "services.geocoder",
    "services.routing",
    "services.nearby",
    # Capabilities the architecture spine names: interpolation is a headline
    # feature, the watchers are pipeline stages bound by AD-10, google_maps
    # backs /deep. Utilities (geocoder_helpers, progress, traffic_tile) are
    # deliberately absent — their tests live in tests/unit/.
    "shared.interpolation",
    "shared.google_maps",
    "services.gn_watcher",
    "services.oa_watcher",
    "services.watcher",
    # Billing entry points: a regeneration must expose these apps somewhere.
    "billing.main",
    "billing.control_plane",
    "billing.auth",
    "billing.apisix_admin",
    # NOTE: billing.gateway is a tested reference implementation that nothing
    # deploys (see G-6). Declared contractual for now so its 9 tests keep
    # running; whether a regeneration must rebuild it is an open product call.
    "billing.gateway",
}


def _files():
    return sorted(p for p in ACCEPTANCE.rglob("*.py") if p.name != pathlib.Path(__file__).name)


def _production_aliases(tree):
    alias = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            for a in n.names:
                if a.name.split(".")[0] in PRODUCTION:
                    alias[a.asname or a.name.split(".")[0]] = a.name
        elif isinstance(n, ast.ImportFrom) and n.module and n.module.split(".")[0] in PRODUCTION:
            for a in n.names:
                alias[a.asname or a.name] = f"{n.module}.{a.name}"
    return alias


@pytest.mark.parametrize("path", _files(), ids=lambda p: p.name)
def test_no_production_internals(path):
    """No acceptance test may touch a private name on a production module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    alias = _production_aliases(tree)
    offenders = sorted(
        {
            f"{alias[n.value.id]}.{n.attr}"
            for n in ast.walk(tree)
            if isinstance(n, ast.Attribute)
            and n.attr.startswith("_")
            and not n.attr.startswith("__")
            and isinstance(n.value, ast.Name)
            and n.value.id in alias
        }
    )
    assert not offenders, (
        f"{path.name} binds to production internals: {offenders}. "
        "Drive an external surface, read the value from spec/, or move the test "
        "to tests/unit/ if it is genuinely implementation-scoped."
    )


@pytest.mark.parametrize("path", _files(), ids=lambda p: p.name)
def test_imports_stay_within_the_contractual_api(path):
    """Importing a production module that is not declared contractual means the
    contract quietly grew; declare it here or use one that is."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            imported |= {a.name for a in n.names if a.name.split(".")[0] in PRODUCTION}
        elif isinstance(n, ast.ImportFrom) and n.module and n.module.split(".")[0] in PRODUCTION:
            # `from billing import config` contributes billing.config, not billing
            imported |= {
                f"{n.module}.{a.name}" if n.module in PRODUCTION else n.module for a in n.names
            }
    undeclared = sorted(m for m in imported if m not in CONTRACTUAL_MODULES)
    assert not undeclared, (
        f"{path.name} imports undeclared production modules: {undeclared}. "
        "Add them to CONTRACTUAL_MODULES only if a regeneration must keep that "
        "import path."
    )
