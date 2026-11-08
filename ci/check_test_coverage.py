#!/usr/bin/env python3
"""核对八个 root 分片与一个宿主集成任务的唯一覆盖；不签物理内存资格。"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile


ROOT_SHARD_COUNT = 8
HOST_SHARD_INDEX = ROOT_SHARD_COUNT
SHARD_COUNT = ROOT_SHARD_COUNT + 1
SOURCE_REPOSITORY = "william-lbn/autoscaling"
MODULE = "github.com/neondatabase/autoscaling"
HEAVY_PACKAGES = {f"{MODULE}/pkg/dsga/{name}" for name in ("owner", "journal")}
HOST_INTEGRATION_PACKAGES = {f"{MODULE}/{name}" for name in
                             ("pkg/reporting", "pkg/neonvm/controllers/functests")}
TEST_COMMAND = {"race": True, "count": 1, "timeout": "30m", "package_parallelism": 2}


class CoverageError(ValueError):
    """证据缺失、混合身份或执行覆盖不完整时关闭发布门禁。"""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CoverageError(message)


def unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON field: {key}")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    raise CoverageError(f"non-finite JSON number: {value}")


def decode_json(raw: str, label: str) -> dict:
    try:
        value = json.loads(raw, object_pairs_hook=unique_object, parse_constant=reject_constant)
    except (ValueError, TypeError) as error:
        raise CoverageError(f"invalid JSON in {label}: {error}") from error
    require(isinstance(value, dict), f"expected JSON object in {label}")
    return value


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def validate_names(value: object, label: str) -> dict[str, list[str]]:
    require(isinstance(value, dict), f"{label} must be a package map")
    for package, names in value.items():
        require(isinstance(package, str) and
                (package == MODULE or package.startswith(MODULE + "/")) and
                not any(c.isspace() for c in package), f"invalid {label} package: {package}")
        require(isinstance(names, list), f"{label} names must be an array: {package}")
        require(all(isinstance(name, str) and name.isidentifier() and
                    name.startswith(("Test", "Example", "Fuzz")) for name in names),
                f"invalid top-level name in {label}: {package}")
        require(names == sorted(set(names)), f"unsorted or duplicate {label} names: {package}")
    return value


def select_tests(inventory: dict[str, list[str]], index: int) -> dict[str, list[str]]:
    # 独立重算 driver 的稳定分片，不能信任 report 中自报的 selection 或总数。
    selected = {}
    for package, names in inventory.items():
        if package in HOST_INTEGRATION_PACKAGES:
            if index == HOST_SHARD_INDEX:
                selected[package] = names
        elif package not in HEAVY_PACKAGES:
            if index == 0:
                selected[package] = names
        else:
            chosen = [name for name in names if int.from_bytes(
                hashlib.sha256(f"{package}/{name}".encode("utf-8")).digest(), "big"
            ) % ROOT_SHARD_COUNT == index]
            if chosen:
                selected[package] = chosen
    return selected


def expected_commands(selection: dict[str, list[str]]) -> list[dict]:
    commands = []
    ordinary = sorted(package for package in selection if package not in HEAVY_PACKAGES)
    if ordinary:
        commands.append({"packages": ordinary, "run_pattern": "", "exit_code": 0})
    for package in sorted(HEAVY_PACKAGES & selection.keys()):
        pattern = "^(" + "|".join(re.escape(name) for name in selection[package]) + ")$"
        commands.append({"packages": [package], "run_pattern": pattern, "exit_code": 0})
    return commands


def validate_commands(commands: object, selection: dict[str, list[str]], label: str) -> None:
    require(isinstance(commands, list), f"{label}: commands must be an array")
    expected = expected_commands(selection)
    actual = []
    for command in commands:
        require(isinstance(command, dict), f"{label}: invalid command record")
        require(type(command.get("exit_code")) is int and command["exit_code"] == 0,
                f"{label}: command did not exit zero")
        actual.append({key: command.get(key) for key in ("packages", "run_pattern", "exit_code")})
    require(actual == expected, f"{label}: commands do not exactly cover selected packages and names")


def events(path: Path):
    with path.open(encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            if raw.strip():
                yield decode_json(raw, f"{path.name}:{line_number}")


def reason_lines(output: list[str]) -> str:
    # Go 的 --- SKIP 行只有结果，没有原因；只保留测试本身记录的说明。
    # 同时去掉 file.go:line: 前缀，空的 t.Skip("") 不能成为可审计的豁免。
    useful = []
    for raw in output:
        for line in raw.splitlines():
            text = line.strip()
            if not text or re.match(r"^(=== (RUN|PAUSE|CONT|NAME)\b|--- (PASS|FAIL|SKIP):)", text):
                continue
            text = re.sub(r"^.*?\.go:\d+:\s*", "", text)
            if text:
                useful.append(text)
    return "\n".join(useful)


def inspect_events(path: Path, expected: dict[str, list[str]], *, listing: bool) -> dict:
    """只从独立执行日志算 run/terminal；枚举日志绝不算执行成功。"""
    label = str(path)
    packages = {package: {"started": False, "terminal": None, "output": []} for package in expected}
    listed = {package: [] for package in expected}
    tests = {}
    skipped = []
    for event in events(path):
        action = event.get("Action")
        require(isinstance(action, str), f"{label}: missing event action")
        require(action not in {"fail", "build-fail"}, f"{label}: actual failed event: {event}")
        # Go 1.24 可以在 JSON 流中报告编译器输出；它不属于测试执行或枚举。
        if action == "build-output":
            require(isinstance(event.get("ImportPath"), str) and
                    isinstance(event.get("Output"), str) and not event.get("Test"),
                    f"{label}: invalid build-output event")
            continue
        package = event.get("Package")
        require(isinstance(package, str) and package in expected,
                f"{label}: unexpected package event: {package}")
        name = event.get("Test", "")
        require(isinstance(name, str), f"{label}: invalid test name")
        state = packages[package]
        if name:
            require(not listing, f"{label}: listing unexpectedly executed a test")
            require(state["started"] and state["terminal"] is None,
                    f"{label}: test event outside package execution: {package}/{name}")
            top = name.split("/", 1)[0]
            require(top in expected[package], f"{label}: unexpected top-level test: {package}/{top}")
            key = (package, name)
            test = tests.setdefault(key, {"started": False, "terminal": None, "output": []})
            if action == "run":
                require(not test["started"], f"{label}: duplicate test run: {package}/{name}")
                if "/" in name:
                    require(tests.get((package, top), {}).get("started", False),
                            f"{label}: subtest ran before top-level test: {package}/{name}")
                test["started"] = True
            elif action in {"pass", "skip"}:
                require(test["started"] and test["terminal"] is None,
                        f"{label}: missing run or duplicate test terminal: {package}/{name}")
                test["terminal"] = action
                if action == "skip":
                    reason = reason_lines(test["output"])
                    require(bool(reason), f"{label}: skipped test has no reason: {package}/{name}")
                    skipped.append({"package": package, "test": name, "reason": reason})
                test["output"] = []
            elif action == "output":
                require(isinstance(event.get("Output"), str), f"{label}: invalid test output")
                test["output"].append(event["Output"])
            elif action in {"pause", "cont"}:
                require(test["started"] and test["terminal"] is None,
                        f"{label}: pause/cont outside test execution: {package}/{name}")
            else:
                raise CoverageError(f"{label}: unexpected test action: {action}")
            continue
        if action == "start":
            require(not state["started"], f"{label}: duplicate package start: {package}")
            state["started"] = True
        elif action in {"pass", "skip"}:
            require(state["started"] and state["terminal"] is None,
                    f"{label}: missing start or duplicate package terminal: {package}")
            state["terminal"] = action
            if action == "skip":
                require(not expected[package] and any("[no test files]" in text for text in state["output"]),
                        f"{label}: package skip is not an actual no-test-files package: {package}")
                skipped.append({"package": package, "test": None, "reason": "no test files"})
            state["output"] = []
        elif action == "output":
            require(state["started"] and state["terminal"] is None and
                    isinstance(event.get("Output"), str), f"{label}: invalid package output: {package}")
            state["output"].append(event["Output"])
            if listing:
                for raw in event["Output"].splitlines():
                    name = raw.strip()
                    if name.startswith(("Test", "Example", "Fuzz")) and name.isidentifier():
                        require(name not in listed[package], f"{label}: duplicate listed test: {package}/{name}")
                        listed[package].append(name)
        else:
            raise CoverageError(f"{label}: unexpected package action: {action}")
    for package, state in packages.items():
        require(state["started"] and state["terminal"] in {"pass", "skip"},
                f"{label}: incomplete package terminal: {package}")
    if listing:
        require({package: sorted(names) for package, names in listed.items()} == expected,
                f"{label}: actual listed Test/Example/Fuzz inventory does not match report")
    else:
        for package, names in expected.items():
            for name in names:
                require((package, name) in tests, f"{label}: missing selected top-level run: {package}/{name}")
        for (package, name), state in tests.items():
            require(state["started"] and state["terminal"] in {"pass", "skip"},
                    f"{label}: incomplete test run/terminal: {package}/{name}")
    counts = Counter()
    for (_, name), state in tests.items():
        kind = "subtest" if "/" in name else "top_level"
        counts[f"{kind}_{state['terminal']}"] += 1
    for state in packages.values():
        counts[f"package_{state['terminal']}"] += 1
    return {"counts": dict(counts), "skipped": skipped}


def collect(input_dir: Path, source_commit: str, arch: str, execution: dict) -> dict:
    require(bool(re.fullmatch(r"[0-9a-f]{40}", source_commit)), "invalid expected source commit")
    require(arch in {"amd64", "arm64"}, "invalid expected architecture")
    require(isinstance(execution, dict) and set(execution) == {"repository", "run_id", "run_attempt"},
            "invalid expected execution identity")
    require(isinstance(execution["repository"], str) and
            bool(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", execution["repository"])),
            "invalid expected caller repository")
    for key in ("run_id", "run_attempt"):
        require(isinstance(execution[key], str) and bool(re.fullmatch(r"[1-9][0-9]*", execution[key])),
                f"invalid expected {key}")
    require(input_dir.is_dir(), "test artifact input directory is missing")
    root = input_dir.resolve()
    paths = sorted(input_dir.rglob("report.json"))
    require(len(paths) == SHARD_COUNT, f"expected exactly {SHARD_COUNT} shard reports, got {len(paths)}")
    by_index = {}
    inventory = None
    total = Counter()
    skipped = []
    summaries = []
    for path in paths:
        for filename in ("report.json", "list-events.jsonl", "go-events.jsonl"):
            item = path.parent / filename
            require(item.is_file() and not item.is_symlink() and item.resolve().is_relative_to(root),
                    f"missing or unsafe evidence file: {item}")
        report = decode_json(path.read_text(encoding="utf-8"), str(path))
        require(type(report.get("schema_version")) is int and report["schema_version"] == 2,
                f"{path}: unsupported report schema")
        require(report.get("source_repository") == SOURCE_REPOSITORY and
                report.get("source_commit") == source_commit and
                report.get("architecture") == arch and report.get("execution") == execution,
                f"{path}: source, architecture or execution identity mismatch")
        require(type(report.get("shard_count")) is int and report["shard_count"] == SHARD_COUNT,
                f"{path}: invalid shard count")
        require(type(report.get("root_shard_count")) is int and report["root_shard_count"] == ROOT_SHARD_COUNT,
                f"{path}: root shard count changed")
        index = report.get("shard_index")
        require(type(index) is int and 0 <= index < SHARD_COUNT, f"{path}: invalid shard index")
        require(index not in by_index, f"duplicate shard index: {index}")
        by_index[index] = path
        environment = "host-integration" if index == HOST_SHARD_INDEX else "root-readonly"
        require(report.get("execution_environment") == environment,
                f"{path}: execution environment does not match shard lane")
        user = report.get("execution_user")
        require(isinstance(user, dict) and set(user) == {"uid", "gid"} and
                all(type(user[key]) is int and user[key] >= 0 for key in user) and
                (user["uid"] == 0) == (environment == "root-readonly") and
                (environment != "root-readonly" or user["gid"] == 0),
                f"{path}: effective user does not match execution environment")
        command = report.get("test_command")
        require(isinstance(command, dict) and command == TEST_COMMAND and
                all(type(command[key]) is type(value) for key, value in TEST_COMMAND.items()),
                f"{path}: required race/count/timeout/package-parallelism changed")
        require(type(report.get("exit_code")) is int and report["exit_code"] == 0,
                f"{path}: shard did not exit zero")
        current_inventory = validate_names(report.get("inventory"), "inventory")
        required_packages = HEAVY_PACKAGES | HOST_INTEGRATION_PACKAGES
        require(required_packages.issubset(current_inventory) and
                all(current_inventory[package] for package in required_packages),
                f"{path}: missing heavy or host integration package inventory")
        if inventory is None:
            inventory = current_inventory
        require(current_inventory == inventory, f"{path}: shard inventory mismatch")
        selection = validate_names(report.get("selection"), "selection")
        require(selection == select_tests(inventory, index), f"{path}: incorrect stable shard selection")
        validate_commands(report.get("commands"), selection, str(path))
        inspect_events(path.parent / "list-events.jsonl", inventory, listing=True)
        actual = inspect_events(path.parent / "go-events.jsonl", selection, listing=False)
        total.update(actual["counts"])
        skipped.extend({"shard_index": index, **item} for item in actual["skipped"])
        summaries.append({
            "shard_index": index,
            "execution_environment": environment,
            "execution_user": user,
            "selected_packages": len(selection),
            "selected_top_level_tests": sum(map(len, selection.values())),
            "actual": actual["counts"],
            "sha256": {filename: sha256(path.parent / filename) for filename in
                       ("report.json", "list-events.jsonl", "go-events.jsonl")},
        })
    require(set(by_index) == set(range(SHARD_COUNT)), "shard index set is incomplete")
    assert inventory is not None
    expected_count = sum(map(len, inventory.values()))
    require(total["top_level_pass"] + total["top_level_skip"] == expected_count,
            "global top-level coverage is not exactly once")
    kinds = Counter()
    for names in inventory.values():
        for name in names:
            kinds[next(prefix for prefix in ("Test", "Example", "Fuzz") if name.startswith(prefix))] += 1
    return {
        "schema_version": 2,
        "status": "passed",
        "source_repository": SOURCE_REPOSITORY,
        "source_commit": source_commit,
        "architecture": arch,
        "execution": execution,
        "shard_count": SHARD_COUNT,
        "root_shard_count": ROOT_SHARD_COUNT,
        "host_integration_packages": sorted(HOST_INTEGRATION_PACKAGES),
        "scope": "complete-default-linux-go-suite",
        "test_command": TEST_COMMAND,
        "inventory": {
            "packages": len(inventory), "top_level_tests": expected_count,
            "kinds": dict(kinds),
            "sha256": hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":"),
                                                ensure_ascii=False).encode("utf-8")).hexdigest(),
        },
        "actual": {key: total[key] for key in ("top_level_pass", "top_level_skip", "subtest_pass",
                                               "subtest_skip", "package_pass", "package_skip")},
        "skipped": sorted(skipped, key=lambda item: (item["package"], item["test"] or "", item["shard_index"])),
        "shards": sorted(summaries, key=lambda item: item["shard_index"]),
        "normal_qualification": False,
        "physical_memory_release_qualified": False,
    }


def write_result(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=path.name + ".", suffix=".tmp", delete=False) as output:
        temporary = Path(output.name)
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--arch", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    execution = {"repository": args.repository, "run_id": args.run_id, "run_attempt": args.run_attempt}
    try:
        result = collect(args.input_dir, args.source_commit, args.arch, execution)
    except (CoverageError, OSError, UnicodeError) as error:
        # 失败也覆盖本次专用输出，避免重跑时遗留的成功收据被误读为当前结果。
        write_result(args.output, {"schema_version": 2, "status": "failed", "error": str(error),
                                   "source_commit": args.source_commit, "architecture": args.arch,
                                   "execution": execution})
        print(f"Test coverage rejected: {error}", file=sys.stderr)
        return 1
    write_result(args.output, result)
    print(f"Verified {result['inventory']['top_level_tests']} top-level tests across {SHARD_COUNT} shards; "
          f"passed={result['actual']['top_level_pass']}, skipped={result['actual']['top_level_skip']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
