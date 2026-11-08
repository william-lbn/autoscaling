#!/usr/bin/env python3
"""覆盖聚合的拒错测试；合成 JSON 只验证门禁，绝不作为真实 suite 成绩。"""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import check_test_coverage as coverage


OWNER = coverage.MODULE + "/pkg/dsga/owner"
JOURNAL = coverage.MODULE + "/pkg/dsga/journal"
ORDINARY = coverage.MODULE + "/pkg/example"
EMPTY = coverage.MODULE + "/pkg/no_tests"


class CoverageContractTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input = self.root / "artifacts"
        self.input.mkdir()
        self.source = "a" * 40
        self.execution = {"repository": "william-lbn/neon", "run_id": "1234", "run_attempt": "2"}
        self.inventory = {
            OWNER: sorted(f"TestOwner{i}" for i in range(24)),
            JOURNAL: sorted([*(f"TestJournal{i}" for i in range(24)), "FuzzJournal", "TestPrivateFixture"]),
            ORDINARY: ["Example", "TestPlain", "TestSub"],
            EMPTY: [],
        }
        self.write_all()

    def shard(self, package, name):
        return int(hashlib.sha256((package + "/" + name).encode()).hexdigest(), 16) % 8

    def directory(self, index):
        return self.input / f"shard-{index}"

    def selection(self, index):
        selected = {}
        for package, names in self.inventory.items():
            if package not in (OWNER, JOURNAL):
                if index == 0:
                    selected[package] = names
            else:
                chosen = [name for name in names if self.shard(package, name) == index]
                if chosen:
                    selected[package] = chosen
        return selected

    def commands(self, selected):
        result = []
        ordinary = sorted(set(selected) - {OWNER, JOURNAL})
        if ordinary:
            result.append({"packages": ordinary, "run_pattern": "", "exit_code": 0})
        for package in sorted({OWNER, JOURNAL} & selected.keys()):
            result.append({"packages": [package], "run_pattern": "^(" + "|".join(selected[package]) + ")$", "exit_code": 0})
        return result

    def make_events(self, selected, *, listing=False):
        result = []
        for package, names in sorted(selected.items()):
            result.append({"Action": "start", "Package": package})
            if not names:
                result += [{"Action": "output", "Package": package,
                            "Output": f"?   \t{package}\t[no test files]\n"},
                           {"Action": "skip", "Package": package, "Elapsed": 0}]
                continue
            for name in names:
                if listing:
                    result.append({"Action": "output", "Package": package, "Output": name + "\n"})
                    continue
                item = {"Package": package, "Test": name}
                result += [{**item, "Action": "run"},
                           {**item, "Action": "output", "Output": f"=== RUN   {name}\n"}]
                if name == "TestPrivateFixture":
                    result += [{**item, "Action": "output", "Output": "    private_test.go:35: optional private fixture is absent\n"},
                               {**item, "Action": "output", "Output": f"--- SKIP: {name} (0.00s)\n"},
                               {**item, "Action": "skip", "Elapsed": 0}]
                    continue
                if name in ("TestSub", "FuzzJournal"):
                    child = {"Package": package, "Test": name + ("/seed#0" if name.startswith("Fuzz") else "/optional")}
                    result.append({**child, "Action": "run"})
                    if name == "TestSub":
                        result += [{**child, "Action": "output", "Output": "    sub_test.go:12: optional child fixture is absent\n"},
                                   {**child, "Action": "skip", "Elapsed": 0}]
                    else:
                        result.append({**child, "Action": "pass", "Elapsed": 0})
                result.append({**item, "Action": "pass", "Elapsed": 0})
            result.append({"Action": "pass", "Package": package, "Elapsed": 0.01})
        return result

    def write_jsonl(self, path, records):
        path.write_text("".join(json.dumps(record) + "\n" for record in records))

    def read_events(self, index, name="go-events.jsonl"):
        return [json.loads(line) for line in (self.directory(index) / name).read_text().splitlines()]

    def write_all(self):
        for index in range(8):
            directory = self.directory(index)
            directory.mkdir(exist_ok=True)
            selected = self.selection(index)
            report = {
                "schema_version": 1, "source_repository": "william-lbn/autoscaling",
                "source_commit": self.source, "architecture": "amd64", "execution": self.execution,
                "shard_count": 8, "shard_index": index, "test_command": dict(coverage.TEST_COMMAND),
                "inventory": self.inventory, "selection": selected,
                "commands": self.commands(selected), "exit_code": 0,
            }
            self.save_report(index, report)
            self.write_jsonl(directory / "list-events.jsonl", self.make_events(self.inventory, listing=True))
            self.write_jsonl(directory / "go-events.jsonl", self.make_events(selected))

    def read_report(self, index):
        return json.loads((self.directory(index) / "report.json").read_text())

    def save_report(self, index, report):
        (self.directory(index) / "report.json").write_text(json.dumps(report) + "\n")

    def collect(self):
        return coverage.collect(self.input, self.source, "amd64", self.execution)

    def test_complete_default_suite_counts_pass_skip_and_fuzz_separately(self):
        result = self.collect()
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["inventory"]["top_level_tests"], 53)
        self.assertEqual(result["inventory"]["kinds"], {"Test": 51, "Fuzz": 1, "Example": 1})
        self.assertEqual(result["actual"]["top_level_pass"], 52)
        self.assertEqual(result["actual"]["top_level_skip"], 1)
        self.assertEqual(result["actual"]["subtest_skip"], 1)
        self.assertEqual(result["actual"]["package_skip"], 1)
        self.assertEqual(len(result["skipped"]), 3)
        self.assertTrue(all(item["reason"] for item in result["skipped"]))
        self.assertIs(result["normal_qualification"], False)
        self.assertIs(result["physical_memory_release_qualified"], False)
        self.assertEqual([item["shard_index"] for item in result["shards"]], list(range(8)))

    def test_unicode_go_identifier_is_not_omitted(self):
        self.inventory[OWNER] = sorted(self.inventory[OWNER] + ["Test内存回收"])
        self.write_all()
        self.assertEqual(self.collect()["inventory"]["top_level_tests"], 54)

    def test_missing_and_extra_artifact_are_rejected(self):
        shutil.rmtree(self.directory(7))
        with self.assertRaisesRegex(coverage.CoverageError, "exactly 8"):
            self.collect()
        self.write_all()
        shutil.copytree(self.directory(7), self.input / "extra")
        with self.assertRaisesRegex(coverage.CoverageError, "exactly 8"):
            self.collect()

    def test_duplicate_shard_is_rejected(self):
        report = self.read_report(7)
        report["shard_index"] = 0
        self.save_report(7, report)
        with self.assertRaisesRegex(coverage.CoverageError, "duplicate shard"):
            self.collect()

    def test_source_architecture_caller_run_and_attempt_must_match(self):
        original = self.read_report(3)
        changes = {
            "source_repository": "neondatabase/autoscaling", "source_commit": "b" * 40,
            "architecture": "arm64", "execution": dict(self.execution, run_attempt="1"),
        }
        for key, value in changes.items():
            with self.subTest(field=key):
                self.save_report(3, dict(original, **{key: value}))
                with self.assertRaisesRegex(coverage.CoverageError, "identity mismatch"):
                    self.collect()
        for key, value in (("repository", "william-lbn/autoscaling"), ("run_id", "9999"), ("run_id", 1234)):
            with self.subTest(execution_field=key):
                self.save_report(3, dict(original, execution=dict(self.execution, **{key: value})))
                with self.assertRaisesRegex(coverage.CoverageError, "identity mismatch"):
                    self.collect()

    def test_invalid_schema_index_count_and_nonzero_exit_are_rejected(self):
        original = self.read_report(2)
        for key, value in (("schema_version", True), ("schema_version", 2), ("shard_count", 7),
                           ("shard_index", True), ("shard_index", 8), ("exit_code", 1), ("exit_code", False)):
            with self.subTest(field=key, value=value):
                self.save_report(2, dict(original, **{key: value}))
                with self.assertRaises(coverage.CoverageError):
                    self.collect()

    def test_race_count_timeout_parallelism_cannot_be_weakened(self):
        original = self.read_report(1)
        for key, value in (("race", False), ("count", 2), ("count", True), ("timeout", "0"), ("package_parallelism", 8)):
            with self.subTest(field=key):
                self.save_report(1, dict(original, test_command=dict(coverage.TEST_COMMAND, **{key: value})))
                with self.assertRaisesRegex(coverage.CoverageError, "race/count/timeout"):
                    self.collect()

    def test_identical_but_incomplete_report_inventory_cannot_hide_fuzz_or_example(self):
        for package, name in ((JOURNAL, "FuzzJournal"), (ORDINARY, "Example")):
            with self.subTest(name=name):
                self.write_all()
                for index in range(8):
                    report = self.read_report(index)
                    report["inventory"][package].remove(name)
                    report["selection"] = coverage.select_tests(report["inventory"], index)
                    report["commands"] = coverage.expected_commands(report["selection"])
                    self.save_report(index, report)
                with self.assertRaisesRegex(coverage.CoverageError, "listed.*inventory"):
                    self.collect()

    def test_mixed_inventory_and_unsorted_duplicate_names_are_rejected(self):
        original = self.read_report(2)
        for names in (original["inventory"][OWNER][1:], ["TestB", "TestA"], ["TestA", "TestA"], ["BenchmarkA"]):
            with self.subTest(names=names):
                report = copy.deepcopy(original)
                report["inventory"][OWNER] = names
                self.save_report(2, report)
                with self.assertRaises(coverage.CoverageError):
                    self.collect()

    def test_wrong_hash_assignment_or_ordinary_package_on_another_shard_is_rejected(self):
        original = self.read_report(3)
        report = copy.deepcopy(original)
        report["selection"][ORDINARY] = self.inventory[ORDINARY]
        self.save_report(3, report)
        with self.assertRaisesRegex(coverage.CoverageError, "stable shard selection"):
            self.collect()
        report = copy.deepcopy(original)
        report["selection"][OWNER] = [self.inventory[OWNER][0]]
        self.save_report(3, report)
        with self.assertRaisesRegex(coverage.CoverageError, "stable shard selection"):
            self.collect()

    def test_missing_duplicate_failed_and_unanchored_commands_are_rejected(self):
        original = self.read_report(0)
        for mutation in ("missing", "duplicate", "failure", "boolean_exit", "unanchored"):
            with self.subTest(mutation=mutation):
                report = copy.deepcopy(original)
                if mutation == "missing":
                    report["commands"].pop()
                elif mutation == "duplicate":
                    report["commands"].append(report["commands"][0])
                elif mutation in ("failure", "boolean_exit"):
                    report["commands"][0]["exit_code"] = 1 if mutation == "failure" else False
                else:
                    report["commands"][-1]["run_pattern"] = ".*"
                self.save_report(0, report)
                with self.assertRaisesRegex(coverage.CoverageError, "command"):
                    self.collect()

    def test_listing_is_not_execution_evidence(self):
        shutil.copyfile(self.directory(0) / "list-events.jsonl", self.directory(0) / "go-events.jsonl")
        with self.assertRaises(coverage.CoverageError):
            self.collect()

    def test_missing_fuzz_example_or_regular_run_is_rejected(self):
        for package, name in ((JOURNAL, "FuzzJournal"), (ORDINARY, "Example"), (ORDINARY, "TestPlain")):
            with self.subTest(name=name):
                self.write_all()
                index = self.shard(package, name) if package == JOURNAL else 0
                records = [event for event in self.read_events(index)
                           if not (event.get("Package") == package and
                                   event.get("Test", "").split("/")[0] == name)]
                self.write_jsonl(self.directory(index) / "go-events.jsonl", records)
                with self.assertRaisesRegex(coverage.CoverageError, "missing selected top-level run"):
                    self.collect()

    def test_missing_run_missing_terminal_and_duplicate_run_or_terminal_are_rejected(self):
        original = self.read_events(0)
        for action, mode in (("run", "remove"), ("pass", "remove"), ("run", "duplicate"), ("pass", "duplicate")):
            with self.subTest(action=action, mode=mode):
                records = copy.deepcopy(original)
                position = next(i for i, event in enumerate(records) if event.get("Test") == "TestPlain" and event["Action"] == action)
                if mode == "remove":
                    records.pop(position)
                else:
                    records.insert(position, records[position])
                self.write_jsonl(self.directory(0) / "go-events.jsonl", records)
                with self.assertRaises(coverage.CoverageError):
                    self.collect()

    def test_subtest_failure_or_unfinished_subtest_cannot_hide_under_parent_pass(self):
        original = self.read_events(0)
        for mode in ("fail", "unfinished"):
            with self.subTest(mode=mode):
                records = copy.deepcopy(original)
                position = next(i for i, event in enumerate(records) if event.get("Test") == "TestSub/optional" and event["Action"] == "skip")
                if mode == "fail":
                    records[position]["Action"] = "fail"
                else:
                    records.pop(position)
                self.write_jsonl(self.directory(0) / "go-events.jsonl", records)
                with self.assertRaises(coverage.CoverageError):
                    self.collect()

    def test_unexpected_top_level_or_package_is_rejected(self):
        original = self.read_events(0)
        for event in ({"Action": "run", "Package": ORDINARY, "Test": "TestUnselected"},
                      {"Action": "start", "Package": coverage.MODULE + "/unknown"}):
            with self.subTest(event=event):
                position = next(i for i, item in enumerate(original) if item.get("Package") == ORDINARY and item["Action"] == "start")
                records = original[:position + 1] + [event] + original[position + 1:]
                self.write_jsonl(self.directory(0) / "go-events.jsonl", records)
                with self.assertRaisesRegex(coverage.CoverageError, "unexpected"):
                    self.collect()

    def test_package_failure_duplicate_terminal_and_missing_terminal_are_rejected(self):
        original = self.read_events(0)
        for mode in ("fail", "duplicate", "missing"):
            with self.subTest(mode=mode):
                records = copy.deepcopy(original)
                position = next(i for i, event in enumerate(records) if event["Package"] == ORDINARY and event["Action"] == "pass" and not event.get("Test"))
                if mode == "fail":
                    records[position]["Action"] = "fail"
                elif mode == "duplicate":
                    records.insert(position, records[position])
                else:
                    records.pop(position)
                self.write_jsonl(self.directory(0) / "go-events.jsonl", records)
                with self.assertRaises(coverage.CoverageError):
                    self.collect()

    def test_skip_without_reason_cannot_be_excused_by_framework_skip_output(self):
        index = self.shard(JOURNAL, "TestPrivateFixture")
        records = self.read_events(index)
        for event in records:
            if event.get("Test") == "TestPrivateFixture" and "private_test.go" in event.get("Output", ""):
                event["Output"] = "    private_test.go:35: \n"
        self.write_jsonl(self.directory(index) / "go-events.jsonl", records)
        with self.assertRaisesRegex(coverage.CoverageError, "no reason"):
            self.collect()

    def test_subtest_skip_also_requires_reason(self):
        records = [event for event in self.read_events(0)
                   if not (event.get("Test") == "TestSub/optional" and event["Action"] == "output")]
        self.write_jsonl(self.directory(0) / "go-events.jsonl", records)
        with self.assertRaisesRegex(coverage.CoverageError, "no reason"):
            self.collect()

    def test_package_skip_requires_empty_inventory_and_actual_no_test_files_output(self):
        original = self.read_events(0)
        for mode in ("missing_reason", "nonempty"):
            with self.subTest(mode=mode):
                records = copy.deepcopy(original)
                if mode == "missing_reason":
                    records = [event for event in records if not (event["Package"] == EMPTY and event["Action"] == "output")]
                else:
                    for event in records:
                        if event["Package"] == ORDINARY and event["Action"] == "pass" and not event.get("Test"):
                            event["Action"] = "skip"
                self.write_jsonl(self.directory(0) / "go-events.jsonl", records)
                with self.assertRaisesRegex(coverage.CoverageError, "no-test-files"):
                    self.collect()

    def test_list_package_terminal_duplicate_name_and_actual_test_execution_are_checked(self):
        original = self.read_events(0, "list-events.jsonl")
        for mode in ("missing_terminal", "duplicate_name", "test_execution"):
            with self.subTest(mode=mode):
                records = copy.deepcopy(original)
                if mode == "missing_terminal":
                    records = [event for event in records if not (event["Package"] == ORDINARY and event["Action"] == "pass")]
                elif mode == "duplicate_name":
                    position = next(i for i, event in enumerate(records) if event.get("Output") == "Example\n")
                    records.insert(position, records[position])
                else:
                    position = next(i for i, event in enumerate(records) if event["Package"] == ORDINARY and event["Action"] == "start")
                    records.insert(position + 1, {"Action": "run", "Package": ORDINARY, "Test": "Example"})
                self.write_jsonl(self.directory(0) / "list-events.jsonl", records)
                with self.assertRaises(coverage.CoverageError):
                    self.collect()

    def test_go_build_output_is_not_counted_as_test_and_build_failure_is_rejected(self):
        original = self.read_events(0)
        event = {"Action": "build-output", "ImportPath": "example.org/dependency", "Output": "compiler diagnostic\n"}
        self.write_jsonl(self.directory(0) / "go-events.jsonl", [event] + original)
        self.assertEqual(self.collect()["actual"]["top_level_pass"], 52)
        event["Action"] = "build-fail"
        self.write_jsonl(self.directory(0) / "go-events.jsonl", [event] + original)
        with self.assertRaisesRegex(coverage.CoverageError, "failed event"):
            self.collect()

    def test_malformed_duplicate_fields_and_missing_evidence_are_rejected(self):
        path = self.directory(0) / "go-events.jsonl"
        original = path.read_text()
        for prefix in ('not-json\n', '[]\n', '{"Action":"fail","Action":"output"}\n'):
            with self.subTest(prefix=prefix):
                path.write_text(prefix + original)
                with self.assertRaises(coverage.CoverageError):
                    self.collect()
        path.unlink()
        with self.assertRaisesRegex(coverage.CoverageError, "missing or unsafe"):
            self.collect()

    def test_symlink_evidence_is_rejected(self):
        path = self.directory(0) / "go-events.jsonl"
        external = self.root / "external.jsonl"
        path.replace(external)
        path.symlink_to(external)
        with self.assertRaisesRegex(coverage.CoverageError, "missing or unsafe"):
            self.collect()

    def test_self_reported_counts_cannot_override_real_events(self):
        report = self.read_report(0)
        report["passed_tests"] = 999999
        report["failed_tests"] = 0
        self.save_report(0, report)
        self.assertEqual(self.collect()["actual"]["top_level_pass"], 52)

    def test_cli_writes_success_and_replaces_stale_success_on_failure(self):
        output = self.root / "coverage.json"
        command = [sys.executable, str(Path(coverage.__file__)), "--input-dir", str(self.input),
                   "--source-commit", self.source, "--arch", "amd64", "--repository", self.execution["repository"],
                   "--run-id", "1234", "--run-attempt", "2", "--output", str(output)]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(output.read_text())["status"], "passed")
        shutil.rmtree(self.directory(7))
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(output.read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
