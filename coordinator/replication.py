"""
NEXUS — Replication Manager
Determines which nodes should hold each chunk, honouring the configured
replication factor and current node health.
"""

from __future__ import annotations

import itertools
import logging
import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nodes.heartbeat import HeartbeatMonitor

import config as cfg

logger = logging.getLogger(__name__)


class ReplicationManager:
    """
    Assigns chunk replicas across healthy storage nodes.

    Strategy: spread replicas as evenly as possible using a round-robin
    cursor over the healthy node pool.  On each assignment call the cursor
    advances so consecutive chunks land on different primary nodes.
    """

    def __init__(
        self,
        node_names: list[str],
        heartbeat: "HeartbeatMonitor",
        replication_factor: int = cfg.REPLICATION_FACTOR,
    ) -> None:
        self._all_nodes         = node_names
        self._heartbeat         = heartbeat
        self._replication_factor = replication_factor
        self._rr_iter            = itertools.cycle(node_names)

    def assign_chunk(self, chunk_id: str) -> list[str]:
        """
        Return an ordered list of node names for *chunk_id*.

        The first node is the primary; subsequent nodes are replicas.
        Raises RuntimeError if fewer than *replication_factor* healthy nodes
        are available (system is under-replicated — caller may still proceed
        with whatever healthy nodes exist).
        """
        healthy = self._heartbeat.healthy_nodes()

        if not healthy:
            raise RuntimeError("No healthy nodes available — cannot store chunk")

        if len(healthy) < self._replication_factor:
            logger.warning(
                "Only %d healthy node(s) available; "
                "replication factor=%d — under-replicated!",
                len(healthy),
                self._replication_factor,
            )

        # Pick `replication_factor` distinct healthy nodes, starting from the
        # current round-robin position.
        selected: list[str] = []
        seen_start = None
        for candidate in self._rr_iter:
            if candidate == seen_start:
                break  # full cycle without enough nodes
            if seen_start is None:
                seen_start = candidate
            if candidate in healthy and candidate not in selected:
                selected.append(candidate)
            if len(selected) == min(self._replication_factor, len(healthy)):
                break

        logger.debug("Chunk %s → nodes %s", chunk_id, selected)
        return selected

    def validate_replication(
        self,
        chunk_nodes: list[str],
    ) -> tuple[bool, int]:
        """
        Check whether *chunk_nodes* satisfies the replication factor.

        Returns (ok: bool, live_replica_count: int).
        """
        healthy = set(self._heartbeat.healthy_nodes())
        live    = [n for n in chunk_nodes if n in healthy]
        ok      = len(live) >= self._replication_factor
        return ok, len(live)

    def find_under_replicated(
        self,
        all_chunk_nodes: dict[str, list[str]],
    ) -> list[str]:
        """
        Return chunk IDs where the live replica count is below the factor.

        Parameters
        ----------
        all_chunk_nodes:
            Mapping of chunk_id → list of node names that should hold it.
        """
        under: list[str] = []
        for chunk_id, nodes in all_chunk_nodes.items():
            ok, _ = self.validate_replication(nodes)
            if not ok:
                under.append(chunk_id)
        return under
