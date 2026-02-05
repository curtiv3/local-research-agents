from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
LOCK_PATH = DATA_DIR / ".lock"
STATE_PATH = DATA_DIR / "state.json"
THEORY_PATH = DATA_DIR / "theory.json"
UNSURE_PATH = DATA_DIR / "unsure.json"

CHECK_INTERVAL_SECONDS = 60
BATCH_THRESHOLD = 25
MAX_AGE_SECONDS = 6 * 60 * 60


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def acquire_lock(timeout_seconds: int = 30) -> None:
    start = time.time()
    while True:
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            os.close(fd)
            return
        except FileExistsError:
            if time.time() - start > timeout_seconds:
                raise TimeoutError("scheduler could not acquire data lock")
            time.sleep(0.1)


def release_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def get_counts() -> Dict[str, int]:
    theory = read_json(THEORY_PATH, [])
    unsure = read_json(UNSURE_PATH, [])
    return {"theory": len(theory), "unsure": len(unsure)}


def ensure_reasoner_state(state: Dict[str, Any]) -> Dict[str, Any]:
    reasoner = state.get("reasoner") or {}
    reasoner.setdefault("last_run_ts", None)
    reasoner.setdefault("last_validation_ts", None)
    reasoner.setdefault("last_processed_counts", {"theory": 0, "unsure": 0, "total": 0})
    reasoner.setdefault("last_seen_counts", {"theory": 0, "unsure": 0})
    state["reasoner"] = reasoner
    return state


def read_state_and_counts() -> tuple[Dict[str, Any], Dict[str, int]]:
    acquire_lock()
    try:
        state = ensure_reasoner_state(
            read_json(
                STATE_PATH,
                {
                    "last_collector_run": None,
                    "last_reasoner_run": None,
                    "mode": "offline",
                    "notes": "",
                    "last_query_index": 0,
                    "last_ts": None,
                    "counters": {"FACT": 0, "THEORY": 0, "UNSURE": 0, "TRASH": 0, "TOTAL": 0},
                    "reasoner": {
                        "last_run_ts": None,
                        "last_processed_counts": {"theory": 0, "unsure": 0, "total": 0},
                        "last_validation_ts": None,
                        "last_seen_counts": {"theory": 0, "unsure": 0},
                    },
                },
            )
        )
        counts = get_counts()
        return state, counts
    finally:
        release_lock()


def should_trigger(state: Dict[str, Any], counts: Dict[str, int]) -> tuple[bool, str, int]:
    reasoner = state["reasoner"]
    last_seen = reasoner.get("last_seen_counts") or {"theory": 0, "unsure": 0}
    prev_total = int(last_seen.get("theory", 0)) + int(last_seen.get("unsure", 0))
    curr_total = counts["theory"] + counts["unsure"]
    new_items = max(0, curr_total - prev_total)

    last_run = parse_ts(reasoner.get("last_run_ts") or state.get("last_reasoner_run"))
    age_seconds = None if last_run is None else int((now_utc() - last_run).total_seconds())

    if new_items >= BATCH_THRESHOLD:
        return True, f"new_items={new_items} threshold={BATCH_THRESHOLD}", new_items

    if last_run is None:
        return True, "reasoner has never run", new_items

    if age_seconds is not None and age_seconds >= MAX_AGE_SECONDS:
        return True, f"last_run_age={age_seconds}s >= {MAX_AGE_SECONDS}s", new_items

    return False, f"idle new_items={new_items} age_seconds={age_seconds}", new_items


def update_last_seen_counts(counts: Dict[str, int]) -> None:
    acquire_lock()
    try:
        state = ensure_reasoner_state(read_json(STATE_PATH, {}))
        state["reasoner"]["last_seen_counts"] = {"theory": counts["theory"], "unsure": counts["unsure"]}
        write_json(STATE_PATH, state)
    finally:
        release_lock()


def run_reasoner_once() -> int:
    cmd = ["python", str(ROOT_DIR / "reasoner" / "run_reasoner.py")]
    completed = subprocess.run(cmd, cwd=str(ROOT_DIR), text=True)
    return completed.returncode


def main() -> None:
    print("[scheduler] started: checking reasoner trigger every 60s")
    while True:
        state, counts = read_state_and_counts()
        trigger, reason, _new_items = should_trigger(state, counts)

        if trigger:
            print(f"[scheduler] trigger reasoner: {reason}")
            code = run_reasoner_once()
            refreshed_counts = get_counts()
            update_last_seen_counts(refreshed_counts)
            if code != 0:
                print(f"[scheduler] reasoner exited with code={code}")
        else:
            print(f"[scheduler] {reason}; sleeping {CHECK_INTERVAL_SECONDS}s")

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
