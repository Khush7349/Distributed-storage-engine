"""
NEXUS — Networking Protocol
Request/response schemas for coordinator–node communication.
All communication in this simulation is in-process (function calls), but the
schemas mirror what a real socket-based protocol would look like so the code
remains portable to a networked implementation.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ─── Message types ────────────────────────────────────────────────────────────

class MessageType(str, Enum):
    STORE_CHUNK       = "STORE_CHUNK"
    RETRIEVE_CHUNK    = "RETRIEVE_CHUNK"
    DELETE_CHUNK      = "DELETE_CHUNK"
    LIST_CHUNKS       = "LIST_CHUNKS"
    NODE_HEARTBEAT    = "NODE_HEARTBEAT"
    ACK               = "ACK"
    ERROR             = "ERROR"


class StatusCode(str, Enum):
    OK             = "OK"
    NOT_FOUND      = "NOT_FOUND"
    CHECKSUM_FAIL  = "CHECKSUM_FAIL"
    NODE_DOWN      = "NODE_DOWN"
    INTERNAL_ERROR = "INTERNAL_ERROR"


# ─── Message envelopes ────────────────────────────────────────────────────────

@dataclass
class Request:
    message_type: MessageType
    payload: dict[str, Any]
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)


@dataclass
class Response:
    status: StatusCode
    payload: dict[str, Any]
    request_id: str = ""
    elapsed_ms: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == StatusCode.OK


# ─── Convenience constructors ─────────────────────────────────────────────────

def make_store_request(chunk_id: str, data: bytes, checksum: str) -> Request:
    return Request(
        message_type=MessageType.STORE_CHUNK,
        payload={"chunk_id": chunk_id, "data": data, "checksum": checksum},
    )


def make_retrieve_request(chunk_id: str) -> Request:
    return Request(
        message_type=MessageType.RETRIEVE_CHUNK,
        payload={"chunk_id": chunk_id},
    )


def make_ok(request_id: str, payload: dict[str, Any], elapsed_ms: float = 0.0) -> Response:
    return Response(
        status=StatusCode.OK,
        payload=payload,
        request_id=request_id,
        elapsed_ms=elapsed_ms,
    )


def make_error(
    request_id: str,
    code: StatusCode,
    message: str,
    elapsed_ms: float = 0.0,
) -> Response:
    return Response(
        status=code,
        payload={"error": message},
        request_id=request_id,
        elapsed_ms=elapsed_ms,
    )
