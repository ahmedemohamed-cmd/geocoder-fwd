"""Implementation detail: how the free-endpoint regex is wrapped for APISIX.

The *contract* — which paths bill and which do not — lives in
tests/acceptance/billing/test_free_endpoints.py. This pins the internal helper
that builds the APISIX filter, and is expected to be rewritten with the code.
"""

from billing import apisix_admin, config


def test_free_filter_is_a_negated_uri_regex():
    assert apisix_admin._free_filter() == {
        "_meta": {"filter": [["uri", "!", "~~", config.free_endpoints_regex()]]}
    }
