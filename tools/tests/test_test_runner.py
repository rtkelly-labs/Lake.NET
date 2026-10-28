"""Exercise the documented runner with isolated command stubs; no Docker or SDK required."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class TestRunner(unittest.TestCase):
    def setUp(self):
        (ROOT / "temp").mkdir(exist_ok=True)
        self.work = tempfile.TemporaryDirectory(dir=ROOT / "temp")
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name)
        shutil.copy2(ROOT / "run-tests.sh", self.root)
        for script in ("emulation/up.sh", "tools/test-projects.sh"):
            p = self.root / script
            p.parent.mkdir(exist_ok=True)
            p.write_text('#!/usr/bin/env bash\nprintf "%s|%s|%s|%s\\n" "$0 $*" "${ICEBERG_TEST_TARGET:-}" "${ICEBERG_REQUIRE_TARGET:-}" "${ICEBERG_WAREHOUSE:-}" >> "$TRACE"\nexit "${STUB_EXIT:-0}"\n')
            p.chmod(0o755)
        self.trace = self.root / "trace"

    def run_mode(self, mode, **extra):
        env = {k: v for k, v in os.environ.items() if not k.startswith("ICEBERG_")}
        env.update(TRACE=str(self.trace), **extra)
        result = subprocess.run(["bash", "run-tests.sh", mode], cwd=self.root, env=env, capture_output=True, text=True, check=False)
        return result, self.trace.read_text().splitlines() if self.trace.exists() else []

    def test_emulators_start_before_any_test_and_all_suites_use_selected_target(self):
        result, calls = self.run_mode("emulation")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("emulation/up.sh --spark --trino --polaris --gravitino --nessie --lakekeeper", calls[0])
        for call, suite in zip(calls[1:], ("unit", "cross-engine", "regression"), strict=True):
            self.assertIn(f"test {suite}|emulation|1|", call)

    def test_dependency_failure_prevents_tests(self):
        result, calls = self.run_mode("emulation", STUB_EXIT="7")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(calls), 1)

    def test_aws_selects_target_before_tests_without_starting_emulators(self):
        result, calls = self.run_mode("aws", ICEBERG_WAREHOUSE="s3://test/warehouse")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all("|aws|1|s3://test/warehouse" in call for call in calls))

    def test_spec_only_runs_unit(self):
        result, calls = self.run_mode("spec")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertIn("test unit", calls[0])

    def test_invalid_mode_and_missing_aws_warehouse_fail_before_tests(self):
        for mode in ("unknown", "aws"):
            result, calls = self.run_mode(mode)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
