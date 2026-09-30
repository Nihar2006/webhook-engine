"""
app/core/security.py
~~~~~~~~~~~~~~~~~~~~
Cryptographic utilities for webhook payload signing -- Phase 7.

Signature format (Stripe-compatible)
-------------------------------------
The signature header value is::

    X-Webhook-Signature: t=<unix_timestamp>,v1=<hmac_sha256_hex>

Where the signed payload is::

    f"t={timestamp}.{raw_body_str}"

This mirrors the Stripe webhook signature scheme so that subscribers
familiar with Stripe integration can apply the same verification pattern.

Functions
---------
generate_webhook_signature(secret, timestamp, payload_str) -> str
    Sign a payload string and return the full header value.

verify_webhook_signature(secret, header_value, raw_body, tolerance) -> bool
    Parse, timestamp-check, and constant-time HMAC verify an incoming
    signature header.

Security properties
-------------------
* HMAC-SHA256 -- unforgeable without the shared secret.
* Timestamp embedding -- the timestamp is part of the signed message,
  so an attacker cannot strip or alter it without invalidating the signature.
* Replay prevention -- tolerance (default 300 s) limits how long a
  captured request can be replayed.
* Timing-attack resistance -- hmac.compare_digest does a constant-time
  comparison; a naive == leaks information about how many leading bytes
  match, allowing an attacker to reconstruct the correct digest byte-by-byte.

Dependencies
------------
Stdlib only: hmac, hashlib, time. No third-party packages.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time

logger = logging.getLogger(__name__)


def generate_webhook_signature(
    secret: str,
    timestamp: int,
    payload_str: str,
) -> str:
    """
    Sign a serialised webhook payload and return the X-Webhook-Signature
    header value.

    Parameters
    ----------
    secret:
        The endpoint's shared secret (UTF-8 encoded before hashing).
    timestamp:
        Unix timestamp (integer seconds) of dispatch. Embedded in the
        signed message so the subscriber can validate freshness.
    payload_str:
        The raw JSON body *exactly as it will be sent over the wire*
        (same bytes the subscriber will receive).

    Returns
    -------
    str
        Header value in the form ``t=<timestamp>,v1=<hmac_hex>``.

    Example
    -------
    >>> sig = generate_webhook_signature("my-secret", 1700000000, '{"event":"test"}')
    >>> sig.startswith("t=1700000000,v1=")
    True
    """
    signed_payload = f"t={timestamp}.{payload_str}"
    digest = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify_webhook_signature(
    secret: str,
    header_value: str,
    raw_body: bytes,
    tolerance: int = 300,
) -> bool:
    """
    Verify an inbound X-Webhook-Signature header value.

    Parameters
    ----------
    secret:
        The endpoint's shared secret.
    header_value:
        The full X-Webhook-Signature header string, e.g.
        ``"t=1700000000,v1=abc123..."``.
    raw_body:
        The raw request body bytes *exactly as received* (before any
        JSON parsing).
    tolerance:
        Maximum allowed age of the signature in seconds (default 300 s / 5 min).
        Set to 0 to disable freshness checking (tests only).

    Returns
    -------
    bool
        True if the signature is valid and fresh, False otherwise.
        Never raises -- parse errors return False and are logged at WARNING.

    Security notes
    --------------
    * hmac.compare_digest is used for constant-time comparison.
    * Freshness check uses the *signed* timestamp (embedded in the header),
      not a separate header -- an attacker cannot swap the timestamp without
      invalidating the digest.
    """
    # ------------------------------------------------------------------
    # 1. Parse t= and v1= from the header value.
    # ------------------------------------------------------------------
    timestamp: int | None = None
    received_digest: str | None = None

    try:
        for part in header_value.split(","):
            part = part.strip()
            if part.startswith("t="):
                timestamp = int(part[2:])
            elif part.startswith("v1="):
                received_digest = part[3:]
    except (ValueError, AttributeError) as exc:
        logger.warning("[security] Failed to parse signature header %r: %s", header_value, exc)
        return False

    if timestamp is None or received_digest is None:
        logger.warning(
            "[security] Signature header missing t= or v1= field: %r", header_value
        )
        return False

    # ------------------------------------------------------------------
    # 2. Validate timestamp freshness.
    # ------------------------------------------------------------------
    if tolerance > 0:
        age = abs(int(time.time()) - timestamp)
        if age > tolerance:
            logger.warning(
                "[security] Signature timestamp too old: age=%ds > tolerance=%ds",
                age, tolerance,
            )
            return False

    # ------------------------------------------------------------------
    # 3. Recompute expected HMAC over the exact same signed payload.
    # ------------------------------------------------------------------
    try:
        body_str = raw_body.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning("[security] Request body is not valid UTF-8 -- cannot verify signature")
        return False

    signed_payload = f"t={timestamp}.{body_str}"
    expected_digest = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    # ------------------------------------------------------------------
    # 4. Constant-time comparison -- NEVER use plain == here.
    #    hmac.compare_digest prevents timing side-channel attacks by
    #    always comparing all bytes regardless of where they first differ.
    # ------------------------------------------------------------------
    match = hmac.compare_digest(expected_digest, received_digest)

    if not match:
        logger.warning("[security] HMAC mismatch for timestamp=%d", timestamp)

    return match
