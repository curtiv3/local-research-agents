from __future__ import annotations

import json
import os
import re
import string
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.schema import Question, Theory, Unsure, Validation, make_id, now_utc_iso

DATA_DIR = ROOT_DIR / "data"
LOCK_PATH = DATA_DIR / ".lock"

NEGATION_TERMS = {"not", "no", "cannot", "never", "lacks", "fails", "without"}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def acquire_lock(lock_path: Path, timeout_seconds: int = 30) -> None:
    start = time.time()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            os.close(fd)
            return
        except FileExistsError:
            if (time.time() - start) > timeout_seconds:
                raise TimeoutError(f"Could not acquire lock within {timeout_seconds}s")
            time.sleep(0.1)


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def normalize_text(text: str) -> str:
    lowered = (text or "").lower().strip()
    tbl = str.maketrans("", "", string.punctuation)
    lowered = lowered.translate(tbl)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def token_set(text: str) -> set[str]:
    return set(normalize_text(text).split())


def jaccard_similarity(a: str, b: str) -> float:
    sa, sb = token_set(a), token_set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def contains_negation(text: str) -> bool:
    tokens = token_set(text)
    return any(term in tokens for term in NEGATION_TERMS)


def extract_numbers(text: str) -> List[str]:
    return re.findall(r"\b\d+(?:\.\d+)?\b", text or "")


def shared_subject_tokens(a: str, b: str) -> set[str]:
    stop = {"the", "a", "an", "is", "are", "was", "were", "of", "to", "and", "or", "in", "on", "for", "with", "by", "this", "that"}
    sa = token_set(a) - stop
    sb = token_set(b) - stop
    return sa & sb


def is_duplicate(a: str, b: str) -> bool:
    na, nb = normalize_text(a), normalize_text(b)
    if not na or not nb:
        return False
    if na in nb or nb in na:
        return True
    return jaccard_similarity(na, nb) > 0.85


def is_contradiction(a: str, b: str) -> bool:
    na, nb = normalize_text(a), normalize_text(b)
    if not na or not nb:
        return False

    subject_overlap = shared_subject_tokens(na, nb)
    if not subject_overlap:
        return False

    # negation/opposite heuristic
    if contains_negation(na) != contains_negation(nb):
        return True

    # key-term conflicts
    pair_a = "requires" in na and "does not require" in nb
    pair_b = "requires" in nb and "does not require" in na
    pair_c = " is " in f" {na} " and " is not " in f" {nb} "
    pair_d = " is " in f" {nb} " and " is not " in f" {na} "
    if pair_a or pair_b or pair_c or pair_d:
        return True

    # numeric conflict heuristic
    nums_a = extract_numbers(na)
    nums_b = extract_numbers(nb)
    if nums_a and nums_b and set(nums_a) != set(nums_b):
        return True

    return False


def map_record(raw: Dict[str, Any], default_class: str) -> Dict[str, Any]:
    statement = raw.get("statement") or raw.get("content") or ""
    rid = raw.get("id") or make_id(default_class, statement, raw.get("ts", ""))
    source = raw.get("source_url") or raw.get("source") or "NONE"
    evidence = raw.get("evidence_quote") or "NONE"
    confidence = int(raw.get("confidence", 50) or 50)
    return {
        "id": rid,
        "class": raw.get("class", default_class).upper(),
        "statement": statement,
        "source_url": source,
        "evidence_quote": evidence,
        "confidence": max(0, min(100, confidence)),
        "ts": raw.get("ts") or raw.get("collected_at") or now_utc_iso(),
    }


def load_recent_items(limit: int = 50) -> List[Dict[str, Any]]:
    theory_raw = _read_json(DATA_DIR / "theory.json", [])
    unsure_raw = _read_json(DATA_DIR / "unsure.json", [])
    theory = [map_record(item, "THEORY") for item in theory_raw]
    unsure = [map_record(item, "UNSURE") for item in unsure_raw]
    combined = theory + unsure
    combined.sort(key=lambda x: x.get("ts", ""))
    return combined[-limit:]


def confidence_cap(record: Dict[str, Any]) -> int:
    capped = record["confidence"]
    if record.get("source_url") in (None, "", "NONE") or record.get("evidence_quote") in (None, "", "NONE"):
        capped = min(capped, 60)
    return capped


def build_validation(
    event_type: str,
    item_a: Dict[str, Any],
    item_b: Dict[str, Any] | None,
    message: str,
    confidence_delta: int,
) -> Validation:
    related_ids = [item_a["id"]] + ([item_b["id"]] if item_b else [])
    return Validation(
        id=make_id(event_type, *related_ids, now_utc_iso()),
        fact_id=item_a["id"],
        status=event_type,
        rationale=message,
        metadata={
            "related_ids": related_ids,
            "event_type": event_type,
            "confidence_delta": confidence_delta,
            "record_a": {"id": item_a["id"], "class": item_a["class"], "confidence": item_a["confidence"]},
            "record_b": None
            if item_b is None
            else {"id": item_b["id"], "class": item_b["class"], "confidence": item_b["confidence"]},
            "recommended_confidence": {
                item_a["id"]: max(0, confidence_cap(item_a) + confidence_delta),
                **({item_b["id"]: max(0, confidence_cap(item_b) + confidence_delta)} if item_b else {}),
            },
        },
    )


def build_question(question: str, related_ids: Sequence[str], priority: int, reason: str) -> Question:
    return Question(
        id=make_id("question", question, "|".join(related_ids), now_utc_iso()),
        question=question,
        related_ids=list(related_ids),
        priority=max(1, min(5, int(priority))),
        reason=reason,
    )


def analyze_items(items: List[Dict[str, Any]], max_questions: int = 5) -> Tuple[List[Validation], List[Question]]:
    validations: List[Validation] = []
    questions: List[Question] = []

    # Base weak-evidence checks
    for item in items:
        if item.get("source_url") in (None, "", "NONE") or item.get("evidence_quote") in (None, "", "NONE"):
            validations.append(
                build_validation(
                    event_type="weak_evidence",
                    item_a=item,
                    item_b=None,
                    message="Source or evidence missing; cap confidence to 60.",
                    confidence_delta=0,
                )
            )
            if len(questions) < max_questions:
                questions.append(
                    build_question(
                        question=f"What direct source and quote can verify: {item['statement'][:120]}?",
                        related_ids=[item["id"]],
                        priority=3,
                        reason="Claim lacks source/evidence.",
                    )
                )

    # Pairwise duplicate + contradiction checks
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            a, b = items[i], items[j]
            if is_duplicate(a["statement"], b["statement"]):
                validations.append(
                    build_validation(
                        event_type="duplicate",
                        item_a=a,
                        item_b=b,
                        message="Near-duplicate statements (high overlap/containment).",
                        confidence_delta=-10,
                    )
                )
                if len(questions) < max_questions:
                    questions.append(
                        build_question(
                            question="Are these two statements truly distinct claims or duplicates?",
                            related_ids=[a["id"], b["id"]],
                            priority=2,
                            reason="Duplicate detection triggered.",
                        )
                    )

            if is_contradiction(a["statement"], b["statement"]):
                validations.append(
                    build_validation(
                        event_type="contradiction",
                        item_a=a,
                        item_b=b,
                        message="Possible contradiction by negation/opposite/number conflict heuristics.",
                        confidence_delta=-20,
                    )
                )
                if len(questions) < max_questions:
                    questions.append(
                        build_question(
                            question="Which of these conflicting claims is better supported by direct evidence?",
                            related_ids=[a["id"], b["id"]],
                            priority=5,
                            reason="Contradiction detected.",
                        )
                    )

            if len(questions) >= max_questions:
                continue

    # De-duplicate questions by normalized text+ids
    seen = set()
    deduped_questions: List[Question] = []
    for q in questions:
        key = (normalize_text(q.question), tuple(sorted(q.related_ids)))
        if key in seen:
            continue
        seen.add(key)
        deduped_questions.append(q)
        if len(deduped_questions) >= max_questions:
            break

    return validations, deduped_questions


def run_reasoner(limit: int = 50, max_questions: int = 5) -> Dict[str, int]:
    items = load_recent_items(limit=limit)
    validations, questions = analyze_items(items, max_questions=max_questions)

    acquire_lock(LOCK_PATH)
    try:
        validation_path = DATA_DIR / "validation.json"
        questions_path = DATA_DIR / "questions.json"
        state_path = DATA_DIR / "state.json"

        existing_validations = _read_json(validation_path, [])
        existing_questions = _read_json(questions_path, [])

        existing_validations.extend(v.to_dict() for v in validations)
        existing_questions.extend(q.to_dict() for q in questions)

        _write_json(validation_path, existing_validations)
        _write_json(questions_path, existing_questions)

        state = _read_json(
            state_path,
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
                },
            },
        )

        now_ts = now_utc_iso()
        state["last_reasoner_run"] = now_ts
        reasoner_state = state.get("reasoner") or {}
        reasoner_state["last_run_ts"] = now_ts
        reasoner_state["last_processed_counts"] = {
            "theory": sum(1 for x in items if x["class"] == "THEORY"),
            "unsure": sum(1 for x in items if x["class"] == "UNSURE"),
            "total": len(items),
        }
        reasoner_state["last_validation_ts"] = now_ts if validations else reasoner_state.get("last_validation_ts")
        state["reasoner"] = reasoner_state

        _write_json(state_path, state)
    finally:
        release_lock(LOCK_PATH)

    return {
        "items_processed": len(items),
        "validations_added": len(validations),
        "questions_added": len(questions),
    }
