"""The cross-module public contract.

`spec/module-api.toml` lists every name one production module imports from
another. A regeneration must keep those import paths and names; everything else
about a module's layout is free.

This test exists because the regeneration experiment failed on an import, not
on behaviour: `shared/autocomplete.py` needs `categories.CATEGORY_QUERY_TERMS`,
a derived export that appeared in no spec. The data had been specified; the
surface had not.
"""

import importlib

import pytest

from shared.spec import load

SPEC = load("module-api.toml")


@pytest.mark.parametrize("module", sorted(SPEC))
def test_module_exports_its_contracted_names(module):
    try:
        mod = importlib.import_module(module)
    except ImportError as e:  # a heavy optional dep, not a contract failure
        pytest.skip(f"{module} not importable here: {e}")
    missing = [name for name in SPEC[module] if not hasattr(mod, name)]
    assert not missing, (
        f"{module} is missing contracted exports: {missing}. Another module "
        "imports these; a regeneration that drops them breaks the caller."
    )


def test_spec_covers_every_cross_module_import():
    """The spec must not fall behind the code: any new cross-module import has
    to be declared, or the contract silently stops describing the system."""
    import ast
    import collections
    import pathlib

    prod = ("services", "shared", "billing")
    found = collections.defaultdict(set)
    for root in prod:
        for f in pathlib.Path(root).rglob("*.py"):
            if "__pycache__" in f.as_posix():
                continue
            tree = ast.parse(f.read_text(encoding="utf-8"))
            for n in ast.walk(tree):
                if isinstance(n, ast.ImportFrom) and n.level == 1 and n.module:
                    for a in n.names:
                        if not a.name.startswith("_"):
                            found[f"billing.{n.module}"].add(a.name)
                elif (
                    isinstance(n, ast.ImportFrom)
                    and not n.level
                    and n.module
                    and n.module.split(".")[0] in prod
                ):
                    for a in n.names:
                        if not a.name.startswith("_"):
                            found[n.module].add(a.name)
    undeclared = {
        m: sorted(names - set(SPEC.get(m, [])))
        for m, names in found.items()
        if names - set(SPEC.get(m, []))
    }
    assert not undeclared, (
        f"cross-module imports not declared in spec/module-api.toml: {undeclared}"
    )
