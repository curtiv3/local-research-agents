from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reasoner.reasoner import analyze_items, run_reasoner


def _demo() -> None:
    demo_items = [
        {
            "id": "demo-a",
            "class": "THEORY",
            "statement": "Consciousness requires recurrent processing in cortex.",
            "source_url": "NONE",
            "evidence_quote": "NONE",
            "confidence": 70,
            "ts": "2026-01-01T00:00:00Z",
        },
        {
            "id": "demo-b",
            "class": "UNSURE",
            "statement": "Consciousness does not require recurrent processing in cortex.",
            "source_url": "NONE",
            "evidence_quote": "NONE",
            "confidence": 65,
            "ts": "2026-01-01T00:01:00Z",
        },
    ]

    validations, questions = analyze_items(demo_items, max_questions=5)
    print("[reasoner-demo] Generated validations (in-memory only):")
    print(json.dumps([v.to_dict() for v in validations], indent=2))
    print("[reasoner-demo] Generated questions (in-memory only):")
    print(json.dumps([q.to_dict() for q in questions], indent=2))


def main() -> None:
    if os.getenv("REASONER_DEMO", "0") == "1":
        _demo()
        return

    limit = int(os.getenv("REASONER_LIMIT", "50"))
    summary = run_reasoner(limit=limit, max_questions=5)
    print(
        "[reasoner] run complete "
        f"processed={summary['items_processed']} "
        f"validations_added={summary['validations_added']} "
        f"questions_added={summary['questions_added']}"
    )


if __name__ == "__main__":
    main()
