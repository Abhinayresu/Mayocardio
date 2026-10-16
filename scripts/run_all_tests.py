"""
scripts/run_all_tests.py: Master test runner for MayaCardio Data Pipeline.
Runs all unit, integration, durability, fault-injection, and CLI tests,
reporting execution status and test outcomes.
"""

import os
import sys
import unittest

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def main():
    test_loader = unittest.TestLoader()
    test_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tests"))
    suite = test_loader.discover(start_dir=test_dir, pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 50)
    print(f"Total Tests Executed: {result.testsRun}")
    print(f"Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 50)

    if not result.wasSuccessful():
        sys.exit(1)
    else:
        print("ALL TESTS PASSED WITH 100% SUCCESS!")
        sys.exit(0)


if __name__ == "__main__":
    main()
