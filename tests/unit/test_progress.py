"""ProgressTracker is a logging utility, not a product capability, so its test
is implementation-scoped rather than part of the regeneration contract."""

from shared.progress import ProgressTracker


def test_progress_tracker_counts():
    p = ProgressTracker("test", total=100, log_interval=999)
    p.update(5)
    p.skip(3)
    assert p.count == 5
    assert p.skipped == 3


# ── Run ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback

    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception:
            print(f"  FAIL  {t.__name__}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed out of {passed + failed}")
    raise SystemExit(1 if failed else 0)
