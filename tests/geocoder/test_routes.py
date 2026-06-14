"""Guard the public route inventory.

This is the safety net for refactors that move endpoints around (e.g. splitting
the monolith into routers): the set of (path, methods) the app exposes must not
change silently. Update EXPECTED_ROUTES deliberately when adding/removing an
endpoint.
"""

import services.geocoder as geocoder

_DOCS_PATHS = {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}

EXPECTED_ROUTES = {
    ("/address", ("GET",)),
    ("/autocomplete", ("GET",)),
    ("/deep/forward", ("GET",)),
    ("/deep/reverse", ("GET",)),
    ("/describe", ("GET",)),
    ("/features", ("GET",)),
    ("/feedback", ("POST",)),
    ("/geocode", ("GET",)),
    ("/health", ("GET",)),
    ("/insert", ("POST",)),
    ("/isochrone", ("POST",)),
    ("/locate", ("POST",)),
    ("/optimized_route", ("POST",)),
    ("/places", ("POST",)),
    ("/reverse", ("GET",)),
    ("/route", ("POST",)),
    ("/sources_to_targets", ("POST",)),
    ("/status", ("GET",)),
    ("/traffic/edge", ("GET",)),
    ("/traffic/probe", ("POST",)),
    ("/traffic/probes", ("POST",)),
}


def _actual_routes():
    routes = set()
    for r in geocoder.app.routes:
        if not hasattr(r, "methods") or r.path in _DOCS_PATHS:
            continue
        methods = tuple(sorted(m for m in r.methods if m != "HEAD"))
        routes.add((r.path, methods))
    return routes


def test_route_inventory_is_stable():
    assert _actual_routes() == EXPECTED_ROUTES
