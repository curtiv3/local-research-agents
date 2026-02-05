# Local Research Agents (Scaffold)

This repository is scaffolded for a two-agent local workflow:

- **collector/**: gathers and stores candidate facts.
- **reasoner/**: validates and organizes facts into theory/unsure/trash buckets.
- **shared/**: common schemas and helpers used by both agents.
- **data/**: persistent JSON storage.
- **scripts/**: convenience launch scripts.

## Prerequisites

- Python 3.10+
- Local SearXNG instance
- Local text-generation backend (e.g., oobabooga/text-generation-webui)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run steps

1. Start **SearXNG** (local search endpoint).
2. Start **oobabooga** (local reasoning/model endpoint).
3. Run collector agent (when implemented), e.g.:
   ```bash
   python -m collector
   ```
4. Run reasoner agent (when implemented), e.g.:
   ```bash
   python -m reasoner
   ```

Or use helper scripts in `scripts/`.
