"""Tiny client for Nessie's REST API v2: create, inspect, merge and delete branches.

Nessie gives the lakehouse Git-like version control:
  - a branch is an isolated copy of the catalog (no data is copied, only pointers)
  - you can load data into a branch, check it, then merge it into `main` atomically
  - every commit is kept, so you can see or go back to any earlier state
"""

from __future__ import annotations

import os

import requests

API = os.getenv("NESSIE_API", "http://localhost:19120/api/v2")


def get_ref(name: str) -> dict | None:
    r = requests.get(f"{API}/trees/{name}", timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()["reference"]


def create_branch(name: str, from_ref: str = "main") -> dict:
    existing = get_ref(name)
    if existing:
        delete_branch(name)
    src = get_ref(from_ref)
    r = requests.post(
        f"{API}/trees",
        params={"name": name, "type": "BRANCH"},
        json={"type": "BRANCH", "name": from_ref, "hash": src["hash"]},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["reference"]


def merge_branch(name: str, into: str = "main", message: str | None = None) -> dict:
    target = get_ref(into)
    src = get_ref(name)
    r = requests.post(
        f"{API}/trees/{into}@{target['hash']}/history/merge",
        json={"fromRefName": name, "fromHash": src["hash"], "message": message or f"merge {name} into {into}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def delete_branch(name: str) -> None:
    ref = get_ref(name)
    if ref:
        requests.delete(f"{API}/trees/{name}@{ref['hash']}", timeout=10).raise_for_status()


def log(ref: str = "main", limit: int = 10) -> list[dict]:
    r = requests.get(f"{API}/trees/{ref}/history", params={"max-records": limit}, timeout=10)
    r.raise_for_status()
    return [e["commitMeta"] for e in r.json()["logEntries"]]


def ensure_namespaces(names: list[str], ref: str = "main") -> None:
    """Create namespaces (e.g. bronze/silver/gold) on a branch through the Iceberg REST API.

    Also guarantees `main` has at least one commit, which Nessie needs before
    anything can be merged into it.
    """
    from urllib.parse import quote

    base = os.getenv("NESSIE_ICEBERG", "http://localhost:19120/iceberg/v1")
    prefix = quote(f"{ref}|warehouse", safe="")
    for ns in names:
        r = requests.post(f"{base}/{prefix}/namespaces", json={"namespace": [ns], "properties": {}}, timeout=10)
        if r.status_code not in (200, 409):
            r.raise_for_status()


def create_tag(name: str, from_ref: str = "main") -> dict:
    """Pin the current state of a branch under a readable name (like a Git tag)."""
    existing = get_ref(name)
    if existing:
        requests.delete(f"{API}/trees/{name}@{existing['hash']}", timeout=10).raise_for_status()
    src = get_ref(from_ref)
    r = requests.post(f"{API}/trees", params={"name": name, "type": "TAG"},
                      json={"type": "BRANCH", "name": from_ref, "hash": src["hash"]}, timeout=10)
    r.raise_for_status()
    return r.json()["reference"]


def reset_branch(name: str, to_hash: str) -> dict:
    """Point a branch back at an earlier commit: a catalog-wide rollback of every table at once."""
    ref = get_ref(name)
    r = requests.put(f"{API}/trees/{name}@{ref['hash']}", json={"type": "BRANCH", "name": name, "hash": to_hash}, timeout=10)
    r.raise_for_status()
    return r.json()
