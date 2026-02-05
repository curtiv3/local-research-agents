from __future__ import annotations

import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from urllib.parse import urlencode
from urllib.request import Request, urlopen

import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.schema import Fact, domain, make_hash, make_id, now_utc_iso

SEARX_URL = os.getenv("SEARX_URL", "http://127.0.0.1:8080")
OOBABOOGA_CHAT_URL = os.getenv("OOBABOOGA_CHAT_URL", "http://127.0.0.1:5000/v1/chat/completions")
MODEL_NAME = os.getenv("MODEL_NAME", "local")
INTERVAL_SECONDS = int(os.getenv("COLLECTOR_INTERVAL_SECONDS", "10"))
MAX_CYCLES = int(os.getenv("COLLECTOR_MAX_CYCLES", "0"))  # 0 means run forever
DATA_DIR = ROOT_DIR / "data"
LOCK_PATH = DATA_DIR / ".lock"

CLASS_LABELS = {"FACT", "THEORY", "UNSURE", "TRASH"}

SEED_QUERIES = [
    "integrated information theory consciousness",
    "global workspace theory evidence",
    "neural correlates of consciousness recent findings",
    "panpsychism critiques philosophy of mind",
    "default mode network and self awareness",
    "anesthesia and consciousness biomarkers",
    "sleep dreaming lucidity neuroscience",
    "attention schema theory summary",
    "free energy principle consciousness",
    "ai consciousness debate current arguments",
]


@dataclass
class Episode:
    id: str
    ts: str
    query: str
    search_result_url: str
    search_result_title: str
    class_label: str
    statement: str
    update: str
    source_url: str
    evidence_quote: str
    confidence: int
    reason: str
    raw_model_output: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


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


def html_to_text(html: str) -> str:
    cleaned = re.sub(r"<script.*?>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style.*?>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def searx_search(query: str) -> Optional[Dict[str, Any]]:
    try:
        qs = urlencode({"q": query, "format": "json"})
        req = Request(f"{SEARX_URL}/search?{qs}", headers={"User-Agent": "local-research-agent/1.0"})
        with urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        results = payload.get("results") or []
        return results[0] if results else None
    except Exception:
        return None


def web_open(url: str) -> str:
    try:
        req = Request(url, headers={"User-Agent": "local-research-agent/1.0"})
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        return html_to_text(html)[:8000]
    except Exception:
        return ""


def build_prompt(query: str, result: Dict[str, Any], page_text: str) -> str:
    return (
        "You are a strict classifier for research collection.\\n"
        "Follow this output contract exactly:\\n"
        "UPDATE: <1-4 lines>\\n"
        "CLASS: <FACT|THEORY|UNSURE|TRASH>\\n"
        "STATEMENT: <short>\\n"
        "SOURCE: <url or NONE>\\n"
        "EVIDENCE: \"<direct quote or NONE>\"\\n"
        "CONFIDENCE: <0-100>\\n"
        "REASON: <one line>\\n\\n"
        f"QUERY: {query}\\n"
        f"SEARCH_TITLE: {result.get('title', '')}\\n"
        f"SEARCH_URL: {result.get('url', '')}\\n"
        f"SEARCH_CONTENT: {result.get('content', '')}\\n"
        f"PAGE_TEXT: {page_text[:4000]}"
    )


def call_llm(prompt: str) -> str:
    try:
        payload = json.dumps(
            {
                "model": MODEL_NAME,
                "messages": [
                    {"role": "system", "content": "You return structured blocks only."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
            }
        ).encode("utf-8")
        req = Request(
            OOBABOOGA_CHAT_URL,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "local-research-agent/1.0"},
            method="POST",
        )
        with urlopen(req, timeout=40) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="ignore"))
        return body["choices"][0]["message"]["content"].strip()
    except Exception:
        return (
            "UPDATE: LLM unavailable; recorded placeholder.\n"
            "CLASS: UNSURE\n"
            "STATEMENT: Unable to classify due to model error.\n"
            "SOURCE: NONE\n"
            "EVIDENCE: \"NONE\"\n"
            "CONFIDENCE: 0\n"
            "REASON: Local model endpoint unreachable."
        )


def parse_llm_block(block: str) -> Dict[str, Any]:
    parsed = {
        "update": "",
        "class_label": "UNSURE",
        "statement": "",
        "source": "NONE",
        "evidence": "NONE",
        "confidence": 0,
        "reason": "",
    }
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key, value = key.strip().upper(), value.strip()
        if key == "UPDATE":
            parsed["update"] = value
        elif key == "CLASS":
            parsed["class_label"] = value.upper()
        elif key == "STATEMENT":
            parsed["statement"] = value
        elif key == "SOURCE":
            parsed["source"] = value
        elif key == "EVIDENCE":
            parsed["evidence"] = value.strip('"')
        elif key == "CONFIDENCE":
            m = re.search(r"\d+", value)
            parsed["confidence"] = int(m.group(0)) if m else 0
        elif key == "REASON":
            parsed["reason"] = value

    if parsed["class_label"] not in CLASS_LABELS:
        parsed["class_label"] = "UNSURE"
    parsed["confidence"] = max(0, min(100, int(parsed["confidence"])))
    return parsed


def hard_rule_adjust(parsed: Dict[str, Any], search_url: str) -> Dict[str, Any]:
    source_url = parsed["source"] if parsed["source"] != "NONE" else search_url
    evidence = parsed["evidence"]
    class_label = parsed["class_label"]

    if class_label == "FACT":
        if not source_url or source_url == "NONE" or not evidence or evidence == "NONE":
            class_label = "UNSURE"

    parsed["source"] = source_url if source_url else "NONE"
    parsed["class_label"] = class_label
    return parsed


def append_records(episode: Episode, parsed: Dict[str, Any], query: str) -> None:
    acquire_lock(LOCK_PATH)
    try:
        episodes_path = DATA_DIR / "episodes.json"
        facts_path = DATA_DIR / "facts.json"
        theory_path = DATA_DIR / "theory.json"
        unsure_path = DATA_DIR / "unsure.json"
        trash_path = DATA_DIR / "trash.json"
        state_path = DATA_DIR / "state.json"

        episodes = _read_json(episodes_path, [])
        episodes.append(asdict(episode))
        _write_json(episodes_path, episodes)

        class_label = parsed["class_label"]
        item = {
            "id": make_id(query, parsed["statement"], episode.ts),
            "statement": parsed["statement"],
            "class": class_label,
            "source_url": parsed["source"],
            "evidence_quote": parsed["evidence"],
            "confidence": parsed["confidence"],
            "reason": parsed["reason"],
            "ts": episode.ts,
        }

        if class_label == "FACT":
            fact = Fact(
                id=item["id"],
                content=item["statement"],
                source_url=item["source_url"],
                source_domain=domain(item["source_url"]),
                evidence_quote=item["evidence_quote"],
                metadata={
                    "evidence_quote": item["evidence_quote"],
                    "confidence": item["confidence"],
                    "reason": item["reason"],
                    "class": "FACT",
                },
            )
            facts = _read_json(facts_path, [])
            facts.append(fact.to_dict())
            _write_json(facts_path, facts)
        elif class_label == "THEORY":
            theory = _read_json(theory_path, [])
            theory.append(item)
            _write_json(theory_path, theory)
        elif class_label == "TRASH":
            trash = _read_json(trash_path, [])
            trash.append(item)
            _write_json(trash_path, trash)
        else:
            unsure = _read_json(unsure_path, [])
            unsure.append(item)
            _write_json(unsure_path, unsure)

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
            },
        )
        state["last_query_index"] = state.get("last_query_index", 0)
        state["last_ts"] = episode.ts
        state["last_collector_run"] = episode.ts
        counters = state.get("counters") or {}
        for key in ["FACT", "THEORY", "UNSURE", "TRASH", "TOTAL"]:
            counters[key] = int(counters.get(key, 0))
        counters[class_label] += 1
        counters["TOTAL"] += 1
        state["counters"] = counters
        _write_json(state_path, state)
    finally:
        release_lock(LOCK_PATH)


def load_state() -> Dict[str, Any]:
    return _read_json(
        DATA_DIR / "state.json",
        {
            "last_collector_run": None,
            "last_reasoner_run": None,
            "mode": "offline",
            "notes": "",
            "last_query_index": 0,
            "last_ts": None,
            "counters": {"FACT": 0, "THEORY": 0, "UNSURE": 0, "TRASH": 0, "TOTAL": 0},
        },
    )


def run_cycle(state: Dict[str, Any]) -> None:
    query_index = int(state.get("last_query_index", 0)) % len(SEED_QUERIES)
    query = SEED_QUERIES[query_index]

    result = searx_search(query)
    if result:
        result_url = result.get("url") or ""
        result_title = result.get("title") or ""
        page_text = web_open(result_url)
        prompt = build_prompt(query, result, page_text)
        raw = call_llm(prompt)
        parsed = hard_rule_adjust(parse_llm_block(raw), result_url)
    else:
        result_url = ""
        result_title = ""
        page_text = ""
        raw = (
            "UPDATE: No SearX results for query.\n"
            "CLASS: UNSURE\n"
            "STATEMENT: No search results available this cycle.\n"
            "SOURCE: NONE\n"
            "EVIDENCE: \"NONE\"\n"
            "CONFIDENCE: 0\n"
            "REASON: Search endpoint returned empty or failed."
        )
        parsed = parse_llm_block(raw)

    ts = now_utc_iso()
    episode = Episode(
        id=make_id(query, result_url, ts),
        ts=ts,
        query=query,
        search_result_url=result_url,
        search_result_title=result_title,
        class_label=parsed["class_label"],
        statement=parsed["statement"],
        update=parsed["update"],
        source_url=parsed["source"],
        evidence_quote=parsed["evidence"],
        confidence=parsed["confidence"],
        reason=parsed["reason"],
        raw_model_output=raw,
        metadata={
            "query_hash": make_hash(query),
            "page_text_chars": len(page_text) if result else 0,
        },
    )

    append_records(episode, parsed, query)

    acquire_lock(LOCK_PATH)
    try:
        state_path = DATA_DIR / "state.json"
        current_state = _read_json(state_path, {})
        current_state["last_query_index"] = (query_index + 1) % len(SEED_QUERIES)
        _write_json(state_path, current_state)
    finally:
        release_lock(LOCK_PATH)

    print(
        f"[{ts}] query='{query}' class={parsed['class_label']} "
        f"source={parsed['source']} conf={parsed['confidence']}"
    )


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    cycle = 0
    while True:
        state = load_state()
        run_cycle(state)
        cycle += 1
        if MAX_CYCLES > 0 and cycle >= MAX_CYCLES:
            break
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
