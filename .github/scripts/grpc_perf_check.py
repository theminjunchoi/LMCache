# SPDX-License-Identifier: Apache-2.0
"""Compare a real MP server's gRPC cost between two LMCache builds.

Each build runs a real server in every gRPC mode, and the script measures:

- server CPU time per ping, where the transport is nearly all of the work
- server CPU time per call for the calls vLLM's scheduler makes most
- p99.9 ping latency while those calls keep the server busy, which shows
  short periodic stalls that a p99 would hide

Builds are measured in alternating rounds and compared by median. A check
that fails is measured again and only fails if it fails twice.

Usage:
    python grpc_perf_check.py compare --base-python B --head-python H
    python grpc_perf_check.py measure --mode aio   # run with a build's python
"""

# Standard
from collections.abc import Callable
import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid

MODES = ["sync", "aio", "native"]
THREADS = 4
PING_CALLS = 20000
MIX_CALLS = 8000
PROBE_RATE = 300  # pings per second
PROBE_SECONDS = 10.0
MAX_PING_CPU_INCREASE = 0.10
MAX_MIX_CPU_INCREASE = 0.20
MAX_P999_RATIO = 3.0


def _unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def measure(mode: str) -> dict:
    """Start a server in ``mode`` with this interpreter and measure it."""
    # Third Party
    import psutil

    # First Party
    from lmcache.v1.multiprocess.custom_types import (
        BlockAllocationRecord,
        IPCCacheServerKey,
    )
    from lmcache.v1.multiprocess.token_codec import pack_token_ids
    from lmcache.v1.multiprocess.transport.factory import RequestClientFactory

    port = _unused_port()
    server = subprocess.Popen(
        [sys.executable, "-m", "lmcache.v1.multiprocess.server"]
        + ["--transport", "grpc", "--grpc-server-mode", mode]
        + ["--host", "127.0.0.1", "--port", str(port)]
        + ["--l1-size-gb", "0.1", "--eviction-policy", "LRU"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"grpc://127.0.0.1:{port}"
    client = RequestClientFactory.create(url)
    probe = RequestClientFactory.create(url)
    tokens = pack_token_ids(range(8192))

    def ping() -> None:
        client.ping(1).result(30)

    def scheduler_calls() -> None:
        # The calls vLLM's scheduler makes most, in about the recorded mix.
        request_id = uuid.uuid4().hex
        key = IPCCacheServerKey(
            model_name="perf",
            world_size=1,
            worker_id=None,
            token_bytes=tokens,
            start=0,
            end=8192,
            request_id=request_id,
        )
        record = BlockAllocationRecord(
            req_id=request_id,
            new_block_ids=list(range(16)),
            new_token_ids=list(range(256)),
        )
        client.lookup(key, 1).result(30)
        client.query_prefetch_status(request_id).result(30)
        client.report_block_allocation(1, "perf", [record]).result(30)
        client.end_session(request_id).result(30)

    process = psutil.Process(server.pid)

    def cpu_per_call(work: Callable[[], None], repeats: int, calls: int) -> float:
        before = process.cpu_times()
        threads = [
            threading.Thread(target=lambda: [work() for _ in range(repeats)])
            for _ in range(THREADS)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        after = process.cpu_times()
        used = (after.user + after.system) - (before.user + before.system)
        return used / (repeats * THREADS * calls) * 1e6

    try:
        deadline = time.monotonic() + 120
        while True:
            try:
                if client.ping(1).result(2):
                    break
            except (TimeoutError, ConnectionError):
                if time.monotonic() > deadline or server.poll() is not None:
                    raise
        for _ in range(200):
            scheduler_calls()

        ping_cpu = cpu_per_call(ping, PING_CALLS // THREADS, 1)
        mix_cpu = cpu_per_call(scheduler_calls, MIX_CALLS // 4 // THREADS, 4)

        stop = threading.Event()

        def keep_busy() -> None:
            while not stop.is_set():
                scheduler_calls()

        busy = [threading.Thread(target=keep_busy) for _ in range(2)]
        for thread in busy:
            thread.start()
        latencies = []
        start = time.monotonic()
        while time.monotonic() - start < PROBE_SECONDS:
            sent = time.perf_counter()
            probe.ping(1).result(30)
            latencies.append(time.perf_counter() - sent)
            pause = 1.0 / PROBE_RATE - (time.perf_counter() - sent)
            if pause > 0:
                time.sleep(pause)
        stop.set()
        for thread in busy:
            thread.join()
        return {
            "ping_cpu_us": ping_cpu,
            "mix_cpu_us": mix_cpu,
            "p50_ms": statistics.median(latencies) * 1e3,
            "p999_ms": statistics.quantiles(latencies, n=1000)[998] * 1e3,
        }
    finally:
        client.close()
        probe.close()
        server.kill()
        server.wait()


def _run(python: str, mode: str) -> dict:
    out = subprocess.run(
        [python, __file__, "measure", "--mode", mode],
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "LMCACHE_LOG_LEVEL": "ERROR"},
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def _collect(builds: dict[str, str], modes: list[str], rounds: int) -> dict:
    samples: dict = {(b, m): [] for b in builds for m in modes}
    for _ in range(rounds):
        for mode in modes:
            for build, python in builds.items():
                samples[build, mode].append(_run(python, mode))
    return {
        key: {name: statistics.median(s[name] for s in runs) for name in runs[0]}
        for key, runs in samples.items()
    }


def _verdicts(results: dict, modes: list[str]) -> dict[str, list[str]]:
    failures: dict[str, list[str]] = {}
    for mode in modes:
        base, head = results["base", mode], results["head", mode]
        ping = head["ping_cpu_us"] / base["ping_cpu_us"] - 1
        mix = head["mix_cpu_us"] / base["mix_cpu_us"] - 1
        p999 = head["p999_ms"] / base["p999_ms"]
        reasons = []
        if ping > MAX_PING_CPU_INCREASE:
            reasons.append(f"CPU per ping +{ping:.0%}")
        if mix > MAX_MIX_CPU_INCREASE:
            reasons.append(f"CPU per scheduler call +{mix:.0%}")
        if p999 > MAX_P999_RATIO:
            reasons.append(f"p99.9 latency {p999:.1f}x of base")
        if reasons:
            failures[mode] = reasons
    return failures


def _table(results: dict, modes: list[str]) -> str:
    rows = [
        "| Mode | Build | CPU per ping | CPU per scheduler call"
        " | Latency p50 / p99.9 under load |",
        "|:--|:--|--:|--:|--:|",
    ]
    for mode in modes:
        for build in ("base", "head"):
            r = results[build, mode]
            rows.append(
                f"| {mode} | {build} | {r['ping_cpu_us']:.0f} µs"
                f" | {r['mix_cpu_us']:.0f} µs"
                f" | {r['p50_ms']:.2f} / {r['p999_ms']:.1f} ms |"
            )
    return "\n".join(rows)


def compare(base_python: str, head_python: str, rounds: int) -> int:
    builds = {"base": base_python, "head": head_python}
    results = _collect(builds, MODES, rounds)
    report = ["### gRPC server performance", "", _table(results, MODES)]
    failures = _verdicts(results, MODES)
    if failures:
        # Measure the suspect modes again before failing.
        again = _collect(builds, list(failures), rounds)
        confirmed = _verdicts(again, list(failures))
        report += ["", "Measured again:", "", _table(again, list(failures))]
        failures = {m: r for m, r in failures.items() if m in confirmed}
    if failures:
        report += [""] + [f"- **{m}**: {', '.join(r)}" for m, r in failures.items()]
        report += [
            "",
            "Reproduce with `python .github/scripts/grpc_perf_check.py compare "
            "--base-python <base venv python> --head-python <head venv python>`.",
        ]
    text = "\n".join(report)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as out:
            out.write(text + "\n")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    one = commands.add_parser("measure")
    one.add_argument("--mode", choices=MODES, required=True)
    both = commands.add_parser("compare")
    both.add_argument("--base-python", required=True)
    both.add_argument("--head-python", required=True)
    both.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if args.command == "measure":
        print(json.dumps(measure(args.mode)))
        return 0
    return compare(args.base_python, args.head_python, args.rounds)


if __name__ == "__main__":
    sys.exit(main())
