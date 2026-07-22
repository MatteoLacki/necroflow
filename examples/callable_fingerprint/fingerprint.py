from __future__ import annotations

import hashlib

from necroflow import FingerprintArgs, default_fingerprint

PROJECT_POLICY = "callable-fingerprint-example/v1"


def _add(digest, value: str) -> None:
    encoded = value.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


def project_fingerprint(args: FingerprintArgs) -> str:
    digest = hashlib.sha256()
    _add(digest, PROJECT_POLICY)
    _add(digest, default_fingerprint(args))
    return digest.hexdigest()
