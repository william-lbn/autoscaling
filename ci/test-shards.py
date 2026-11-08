#!/usr/bin/env python3
"""实际默认 Linux Go inventory、确定性分片与原始执行记录；不签物理资格。"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

SHARD_COUNT = 8
HEAVY_PACKAGES = {
    "github.com/neondatabase/autoscaling/pkg/dsga/journal",
    "github.com/neondatabase/autoscaling/pkg/dsga/owner",
}
GO_TEST = ["go", "test", "-json", "-mod=readonly", "-race", "-count=1", "-p", "2", "-timeout=30m"]


def save_report(path: Path, report: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_go(command: list[str], log: Path, stderr_log: Path, *, echo: bool) -> int:
    """保留真实 Go JSON 和独立 stderr；不通过 tee/解析器的成功掩盖子进程退出码。"""
    print("CI command: " + " ".join(command), flush=True)
    with log.open("a", encoding="utf-8") as output, stderr_log.open("a", encoding="utf-8") as errors:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors, text=True)
        assert process.stdout is not None
        for line in process.stdout:
            output.write(line)
            output.flush()
            if echo:
                print(line, end="", flush=True)
        code = process.wait()
    if code:
        print(f"CI command exited {code}; stderr: {stderr_log}", file=sys.stderr, flush=True)
        print(stderr_log.read_text(encoding="utf-8"), file=sys.stderr, flush=True)
    return code


def actual_inventory(directory: Path) -> dict[str, list[str]]:
    # go list 使用当前平台的真实 build constraints；不扫描文件名猜测可运行测试。
    packages = subprocess.check_output(["go", "list", "-mod=readonly", "./..."], text=True).splitlines()
    if not packages or len(set(packages)) != len(packages) or not HEAVY_PACKAGES.issubset(packages):
        raise ValueError("incomplete or duplicate actual package inventory")
    inventory = {package: [] for package in sorted(packages)}
    log = directory / "list-events.jsonl"
    code = run_go(GO_TEST + ["-list", "^(Test|Example|Fuzz)", "./..."], log, directory / "list-stderr.log", echo=False)
    if code:
        raise ValueError("actual Go test enumeration failed")
    finished = {}
    with log.open(encoding="utf-8") as records:
        for raw in records:
            event = json.loads(raw)
            # Go 1.24 的编译器诊断也可进入 JSON；成功编译的诊断不是 package/test。
            # 真正构建失败已由非零命令退出码关闭门禁，聚合器也独立拒绝 build-fail。
            if event.get("Action") == "build-output":
                if not isinstance(event.get("ImportPath"), str) or not isinstance(event.get("Output"), str) or event.get("Test"):
                    raise ValueError("invalid build diagnostic during enumeration")
                continue
            if event.get("Action") == "build-fail":
                raise ValueError("Go enumeration reported a build failure")
            package = event.get("Package")
            if package not in inventory or event.get("Test"):
                raise ValueError("listing returned an unexpected package or executed a test")
            action = event.get("Action")
            if action in {"pass", "skip", "fail"}:
                if package in finished or action == "fail":
                    raise ValueError("duplicate or unsuccessful package enumeration")
                finished[package] = action
            if action == "output":
                name = event.get("Output", "").rstrip("\n")
                if name.startswith(("Test", "Example", "Fuzz")) and name.isidentifier():
                    if name in inventory[package]:
                        raise ValueError("duplicate test name in actual enumeration")
                    inventory[package].append(name)
    if set(finished) != set(inventory):
        raise ValueError("Go enumeration omitted a package terminal status")
    for package in inventory:
        inventory[package].sort()
    if any(not inventory[package] for package in HEAVY_PACKAGES):
        raise ValueError("heavy package returned no runnable tests")
    return inventory


def select_tests(inventory: dict[str, list[str]], index: int) -> dict[str, list[str]]:
    selected = {}
    for package, names in inventory.items():
        if package not in HEAVY_PACKAGES:
            if index == 0:
                selected[package] = names
            continue
        # 顶层函数整体分片，原子场景/子测试/fuzz seed 全部随父执行；不切内部循环。
        chosen = [name for name in names if int.from_bytes(hashlib.sha256(f"{package}/{name}".encode()).digest(), "big") % SHARD_COUNT == index]
        if chosen:
            selected[package] = chosen
    return selected


def main() -> int:
    directory = Path("/test-results")
    index = int(os.environ["CI_TEST_SHARD_INDEX"])
    if not 0 <= index < SHARD_COUNT:
        raise ValueError("invalid shard index")
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if source != os.environ["CI_SOURCE_COMMIT"] or not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("source commit changed before tests")
    # HEAD 相同仍不允许编译被工具改写的 tracked 源码；未跟踪的本次日志不在此检查内。
    subprocess.run(["git", "diff", "--quiet", "HEAD", "--"], check=True)
    report = {
        "schema_version": 1,
        "source_repository": "william-lbn/autoscaling",
        "source_commit": source,
        "architecture": subprocess.check_output(["go", "env", "GOARCH"], text=True).strip(),
        "execution": {
            "repository": os.environ["CI_EXECUTION_REPOSITORY"],
            "run_id": os.environ["CI_EXECUTION_RUN_ID"],
            "run_attempt": os.environ["CI_EXECUTION_RUN_ATTEMPT"],
        },
        "shard_count": SHARD_COUNT,
        "shard_index": index,
        "test_command": {"race": True, "count": 1, "timeout": "30m", "package_parallelism": 2},
        "inventory": {},
        "selection": {},
        "commands": [],
        "exit_code": 1,
    }
    path = directory / "report.json"
    save_report(path, report)
    try:
        inventory = actual_inventory(directory)
        selected = select_tests(inventory, index)
        report["inventory"], report["selection"] = inventory, selected
        save_report(path, report)
        (directory / "go-events.jsonl").touch(exist_ok=False)
        print(f"CI shard {index}/{SHARD_COUNT}: {sum(map(len, selected.values()))} selected top-level tests; {sum(map(len, inventory.values()))} in complete inventory", flush=True)
        plans = []
        ordinary = sorted(package for package in selected if package not in HEAVY_PACKAGES)
        if ordinary:
            plans.append((ordinary, ""))
        for package in sorted(HEAVY_PACKAGES):
            if package in selected:
                pattern = "^(" + "|".join(re.escape(name) for name in selected[package]) + ")$"
                plans.append(([package], pattern))
        code = 0
        for packages, pattern in plans:
            command = GO_TEST + (["-run", pattern] if pattern else []) + packages
            result = run_go(command, directory / "go-events.jsonl", directory / "go-stderr.log", echo=True)
            report["commands"].append({"packages": packages, "run_pattern": pattern, "exit_code": result})
            save_report(path, report)
            if result:
                code = 1
        report["exit_code"] = code
        return code
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
        print(f"CI test shard failed: {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        save_report(path, report)


if __name__ == "__main__":
    sys.exit(main())
