from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


ISO_8601_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def now_utc_iso() -> str:
    """Return current UTC timestamp in a compact ISO-8601 format."""
    return datetime.now(timezone.utc).strftime(ISO_8601_FORMAT)


def make_id(*parts: str) -> str:
    """Create a deterministic ID from one or more string parts."""
    source = "::".join(part.strip() for part in parts if part and part.strip())
    return sha256(source.encode("utf-8")).hexdigest()


def make_hash(payload: str) -> str:
    """Create a content hash for arbitrary payload text."""
    return sha256(payload.encode("utf-8")).hexdigest()


def domain(url: str) -> str:
    """Extract normalized domain from a URL or hostname-like string."""
    parsed = urlparse(url)
    host = parsed.netloc or parsed.path
    return host.lower().strip()


@dataclass
class Fact:
    """Shared fact schema used by both collector and reasoner agents."""

    id: str
    content: str
    source_url: str
    source_domain: str
    collected_at: str = field(default_factory=now_utc_iso)
    content_hash: Optional[str] = None
    evidence_quote: str = "NONE"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.content_hash:
            self.content_hash = make_hash(self.content)
        if not self.source_domain:
            self.source_domain = domain(self.source_url)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Theory:
    id: str
    statement: str
    source_url: str = "NONE"
    evidence_quote: str = "NONE"
    confidence: int = 50
    ts: str = field(default_factory=now_utc_iso)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Unsure:
    id: str
    statement: str
    source_url: str = "NONE"
    evidence_quote: str = "NONE"
    confidence: int = 50
    ts: str = field(default_factory=now_utc_iso)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Question:
    id: str
    question: str
    related_ids: List[str]
    priority: int
    reason: str
    created_ts: str = field(default_factory=now_utc_iso)
    status: str = "open"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Validation:
    """Shared validation schema used by both collector and reasoner agents."""

    id: str
    fact_id: str
    status: str
    rationale: str
    validated_at: str = field(default_factory=now_utc_iso)
    validator: str = "reasoner"
    evidence_quote: str = "NONE"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
