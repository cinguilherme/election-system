#!/usr/bin/env python3
"""Run a chaos scenario against the engine, wait for it, report, and exit non-zero on failure.

Used by `make chaos-run` / `make chaos-load` and by CI. stdlib only, so it runs on
the host and inside the k6 container alike.

Exit codes:
  0  the run passed (baseline steady, every invariant held)
  1  the run failed (a check or the baseline failed)
  2  timeout or transport error
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


def request(method, url, token, payload=None, timeout=15):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-Chaos-Token", token)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode()
        return json.loads(body) if body else {}


def check_line(check):
    mark = "ok  " if check.get("ok?") else "FAIL"
    line = "    %s %-30s %5sms" % (mark, check.get("id"), check.get("duration-ms"))
    probe = (check.get("probe") or {}).get("error")
    if probe:
        line += "\n         probe: %s" % probe
    for assertion in check.get("assertions") or []:
        if not assertion.get("ok?"):
            line += "\n         at %s expected %s %s got %r%s" % (
                assertion.get("path"),
                assertion.get("op"),
                assertion.get("expected"),
                assertion.get("actual"),
                " (%s)" % assertion["error"] if assertion.get("error") else "",
            )
    return line


def print_report(run):
    scenario = (run.get("scenario") or {}).get("name")
    status = run.get("status")
    print("\nrun %s  scenario=%s  status=%s  %ss" % (
        run.get("run-id"), scenario, status.upper(),
        round((run.get("duration-ms") or 0) / 1000.0, 1)))
    if run.get("experiment-id"):
        print("  experiment-id: %s" % run["experiment-id"])

    steady = run.get("steady-state") or []
    if steady:
        print("  baseline")
        for check in steady:
            print(check_line(check))

    steps = run.get("steps") or []
    if steps:
        print("  steps")
        for step in steps:
            print("    %-6s %-24s %s%s" % (
                step.get("action"),
                "ok" if step.get("ok?") else "FAIL",
                "" if step.get("ok?") else (step.get("error") or ""),
                " (%s)" % step.get("fault-id") if step.get("fault-id") else "",
            ))

    invariants = run.get("invariants") or []
    if invariants:
        print("  invariants")
        for check in invariants:
            print(check_line(check))

    if run.get("error"):
        print("  error: %s" % run["error"])
    report = run.get("report") or {}
    if report:
        print("  report: %s" % report.get("json"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario")
    parser.add_argument("--engine", default=os.environ.get("CHAOS_ENGINE_URL", "http://127.0.0.1:8088"))
    parser.add_argument("--token", default=os.environ.get("CHAOS_TOKEN"))
    parser.add_argument("--experiment-id", default=None,
                        help="correlation id; the k6 runner passes the same value via EXP_ID")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--poll", type=float, default=2.0)
    args = parser.parse_args()

    if not args.token:
        print("CHAOS_TOKEN is required", file=sys.stderr)
        return 2

    payload = {}
    if args.experiment_id:
        payload["experiment-id"] = args.experiment_id

    try:
        started = request("POST", "%s/api/scenarios/%s/run" % (args.engine, args.scenario),
                          args.token, payload)
    except urllib.error.HTTPError as exc:
        print("cannot start %s: HTTP %s %s" % (args.scenario, exc.code, exc.read().decode()), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - transport errors are exit code 2
        print("cannot reach the engine at %s: %s" % (args.engine, exc), file=sys.stderr)
        return 2

    run_id = started["run-id"]
    print("started %s (scenario=%s)" % (run_id, args.scenario))
    deadline = time.time() + args.timeout
    while True:
        try:
            run = request("GET", "%s/api/runs/%s" % (args.engine, run_id), args.token)["run"]
        except Exception as exc:  # noqa: BLE001
            print("lost the engine while polling: %s" % exc, file=sys.stderr)
            return 2

        if run.get("status") != "running":
            print_report(run)
            return 0 if run.get("passed?") else 1

        if time.time() > deadline:
            print("%s still running after %ss, aborting" % (run_id, args.timeout), file=sys.stderr)
            try:
                request("POST", "%s/api/runs/%s/abort" % (args.engine, run_id), args.token, {})
            except Exception:  # noqa: BLE001
                pass
            return 2

        time.sleep(args.poll)


if __name__ == "__main__":
    sys.exit(main())
