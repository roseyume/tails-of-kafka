import threading
import os
from confluent_kafka.admin import AdminClient
from typing import List


class AdminManager:
    """Thread-safe manager for the Kafka AdminClient and bootstrap server list.

    Usage:
        manager = AdminManager(initial_list)
        admin = manager.get()
        manager.add("localhost:19092")
        manager.remove("localhost:19092")
    """

    def __init__(self, initial_bootstrap: List[str]):
        self._lock = threading.RLock()
        # normalize entries
        self._bootstrap = [s for s in initial_bootstrap if s]
        self._client = AdminClient({"bootstrap.servers": self.bootstrap_str()})

    def bootstrap_str(self) -> str:
        with self._lock:
            return ",".join(self._bootstrap)

    def get(self) -> AdminClient:
        """Return the current AdminClient instance. Thread-safe."""
        with self._lock:
            return self._client

    def _recreate_client(self):
        with self._lock:
            # (re)create the AdminClient with the current bootstrap list
            self._client = AdminClient({"bootstrap.servers": self.bootstrap_str()})

    def add(self, entry: str) -> bool:
        """Add a bootstrap entry if missing. Returns True if added."""
        with self._lock:
            if entry in self._bootstrap:
                return False
            self._bootstrap.append(entry)
            self._recreate_client()
            return True

    def remove(self, entry: str) -> bool:
        """Remove a bootstrap entry if present. Returns True if removed."""
        with self._lock:
            if entry not in self._bootstrap:
                return False
            self._bootstrap.remove(entry)
            self._recreate_client()
            return True

    def list(self) -> List[str]:
        with self._lock:
            return list(self._bootstrap)
