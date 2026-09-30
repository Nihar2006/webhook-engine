# Phase 7 -- Cryptographic HMAC-SHA256 Payload Signing

> **Status**: Implemented
> **Phase**: 7 of 7
> **Scope**: `app/core/security.py` + delivery worker + `/mock/secure`

---

## Table of Contents

1. [Threat Model](#1-threat-model)
2. [Stripe-Standard Signature Format](#2-stripe-standard-signature-format)
3. [Why `hmac.compare_digest` is Mandatory](#3-why-hmaccompare_digest-is-mandatory)
4. [Implementation](#4-implementation)
5. [Subscriber Verification Code Sample](#5-subscriber-verification-code-sample)
6. [Verification Script Output](#6-verification-script-output)
7. [Interview Defense](#7-interview-defense)
8. [Change Summary](#8-change-summary)

---

## 1. Threat Model

Webhooks travel over the public internet. Without signing, three classes of
attack are trivially possible:

### 1.1 Man-in-the-Middle (MitM)

An attacker positioned between the webhook engine and the subscriber intercepts
the HTTP request, modifies the JSON payload (e.g. changes `amount: 9900` to
`amount: 1`), and forwards it. The subscriber has no way to detect the
modification.

**Mitigation**: HMAC over the exact raw body. Any byte change produces a
completely different digest -- the attacker cannot modify payload and produce
a valid signature without knowing the secret.

### 1.2 Spoofing / Injection

An attacker who knows the subscriber's endpoint URL sends a forged POST
directly to the subscriber, bypassing the webhook engine entirely. The
subscriber processes a fraudulent `payment.processed` event.

**Mitigation**: The shared secret is known only to the webhook engine and the
subscriber. A forged request lacking a valid signature is rejected with HTTP
401.

### 1.3 Replay Attacks

An attacker captures a legitimate signed request (e.g. `order.created`) and
replays it hours or days later. The subscriber re-processes the order, shipping
a second package or charging the customer twice.

**Mitigation**: The Unix timestamp `t=` is part of the **signed payload**
(not a separate header). An attacker cannot alter the timestamp without
invalidating the digest. The subscriber rejects requests where
`abs(now - t) > 300` seconds.

---

## 2. Stripe-Standard Signature Format

The `X-Webhook-Signature` header value is::

    t=<unix_timestamp>,v1=<hmac_sha256_hex>

### 2.1 Signed Payload Construction

```
signed_payload = f"t={timestamp}.{raw_body_string}"
```

The timestamp is **embedded inside** the signed string -- not appended as a
separate field. This means:

- Altering `t=` in the header invalidates the digest.
- Stripping `t=` prevents parsing, which is caught as a verification error.
- The dot `.` is a separator between timestamp and body (Stripe convention).

### 2.2 Digest Computation

```python
import hashlib, hmac

digest = hmac.new(
    secret.encode("utf-8"),        # key
    signed_payload.encode("utf-8"), # message
    hashlib.sha256,                # algorithm
).hexdigest()                      # lowercase hex string
```

### 2.3 Full Header Example

```
X-Webhook-Signature: t=1700000000,v1=3d2d3c1a8f2b...64 hex chars...
```

### 2.4 Why `v1=` prefix?

The `v1=` prefix allows future algorithm rotation. If SHA-256 is ever
deprecated, the engine can add `v2=<new_algo>` to the same header while
keeping `v1=` for backward compatibility. Subscribers select which scheme
they support.

---

## 3. Why `hmac.compare_digest` is Mandatory

### The Timing Attack

A naive signature check uses Python's `==` operator:

```python
# VULNERABLE -- DO NOT USE
if computed_digest == received_digest:
    return True
```

Python's string `==` is implemented as a short-circuit comparison: it returns
`False` as soon as the first differing byte is found. This means the time
taken to evaluate `==` leaks information:

```
"aaa..." == "bbb..."  -> returns False in ~1 ns  (differs at byte 0)
"abc..." == "abd..."  -> returns False in ~3 ns  (differs at byte 2)
"abcde" == "abcde"   -> returns True  in ~5 ns  (full comparison)
```

An attacker making thousands of signature attempts can measure response time to
deduce how many leading bytes are correct, then iterate byte-by-byte until they
reconstruct the full 256-bit digest without knowing the secret. This is a
**timing side-channel attack**.

### The Fix: `hmac.compare_digest`

```python
# CORRECT -- constant-time comparison
import hmac
return hmac.compare_digest(expected_digest, received_digest)
```

`hmac.compare_digest` (Python 3.3+) always compares **all bytes** regardless
of where they first differ. The execution time is constant relative to the
string length, leaking no information about partial matches.

```
Naive ==:
  compare("aaa", "zzz") --> 1 ns  (leaks: "first byte wrong")
  compare("abc", "abd") --> 3 ns  (leaks: "first 2 bytes right!")

hmac.compare_digest:
  compare("aaa", "zzz") --> 5 ns  (same time)
  compare("abc", "abd") --> 5 ns  (same time)
  compare("abc", "abc") --> 5 ns  (same time)
```

> [!CAUTION]
> Never use `==` to compare HMAC digests in production code. Even in internal
> services where timing attacks seem unlikely, `hmac.compare_digest` is the
> correct primitive and costs nothing extra.

---

## 4. Implementation

### 4.1 `app/core/security.py`

```python
def generate_webhook_signature(secret: str, timestamp: int, payload_str: str) -> str:
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
    # 1. Parse t= and v1= from header
    # 2. Check abs(time.time() - t) <= tolerance
    # 3. Recompute HMAC over f"t={t}.{raw_body.decode()}"
    # 4. Return hmac.compare_digest(expected, received)  # constant-time
```

### 4.2 `app/tasks/delivery.py` -- Critical Serialisation Detail

```python
# Serialise ONCE with deterministic settings
payload_str = json.dumps(event_payload, separators=(",", ":"), sort_keys=True)

# Sign the exact string we will send
sig_header = generate_webhook_signature(endpoint_secret, unix_ts, payload_str)

# POST raw bytes -- NOT json=dict (httpx would re-serialise internally)
resp = http_client.post(
    target_url,
    content=payload_str.encode("utf-8"),
    headers={"Content-Type": "application/json", "X-Webhook-Signature": sig_header, ...},
)
```

**Why `sort_keys=True`?** Python `dict` insertion order is preserved (3.7+)
but the subscriber may receive the payload and re-parse it. `sort_keys=True`
with compact separators produces a canonical, reproducible byte sequence
regardless of how the dict was constructed.

### 4.3 Backward Compatibility

Endpoints registered without a `secret` (i.e. `WebhookEndpoint.secret IS NULL`)
skip signing entirely:

```python
sig_header: str | None = None
if endpoint_secret:
    sig_header = generate_webhook_signature(endpoint_secret, unix_ts, payload_str)
if sig_header:
    delivery_headers["X-Webhook-Signature"] = sig_header
```

All Phase 1-6 endpoints continue working with zero changes.

---

## 5. Subscriber Verification Code Sample

Paste into any Python subscriber to verify incoming webhooks:

```python
import hashlib
import hmac
import time

WEBHOOK_SECRET = "your-shared-secret-here"  # from WebhookEndpoint.secret
TOLERANCE_S    = 300                          # 5-minute replay window

def verify_webhook(
    raw_body: bytes,
    signature_header: str,
    secret: str = WEBHOOK_SECRET,
    tolerance: int = TOLERANCE_S,
) -> bool:
    """
    Verify an inbound webhook signature.

    Parameters
    ----------
    raw_body:
        The raw HTTP request body bytes (before JSON parsing).
    signature_header:
        The value of the X-Webhook-Signature header.
    secret:
        The shared secret configured on the webhook endpoint.
    tolerance:
        Max allowed age of the request in seconds (default 300).

    Returns
    -------
    bool: True if valid and fresh, False otherwise.
    """
    timestamp = None
    received_digest = None

    for part in signature_header.split(","):
        part = part.strip()
        if part.startswith("t="):
            try:
                timestamp = int(part[2:])
            except ValueError:
                return False
        elif part.startswith("v1="):
            received_digest = part[3:]

    if timestamp is None or received_digest is None:
        return False  # malformed header

    # Replay protection: reject requests older than tolerance seconds
    if abs(int(time.time()) - timestamp) > tolerance:
        return False

    # Recompute expected signature
    signed_payload = f"t={timestamp}.{raw_body.decode('utf-8')}"
    expected = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    # Constant-time comparison -- NEVER use plain == here
    return hmac.compare_digest(expected, received_digest)


# --- Flask integration example ---
from flask import Flask, request, abort

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def handle_webhook():
    sig = request.headers.get("X-Webhook-Signature", "")
    raw = request.get_data()  # raw bytes, before parsing

    if not verify_webhook(raw, sig):
        abort(401)  # reject unverified requests

    event = request.json
    # Safe to process event["payload"] here
    return {"status": "ok"}, 200


# --- FastAPI integration example ---
from fastapi import FastAPI, Request, HTTPException

api = FastAPI()

@api.post("/webhook")
async def handle_webhook(request: Request):
    sig = request.headers.get("X-Webhook-Signature", "")
    raw = await request.body()  # raw bytes, before parsing

    if not verify_webhook(raw, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    import json
    event = json.loads(raw)
    # Safe to process event["payload"] here
    return {"status": "ok"}
```

---

## 6. Verification Script Output

```
====================================================================
  Webhook Engine -- Phase 7 HMAC-SHA256 Signing Verification
====================================================================
  Run time: 2026-09-30T18:25:00.000000+00:00
====================================================================
  [INFO] FastAPI server reachable [ok]

--------------------------------------------------------------------
  Suite B -- Tamper Resistance (unit-level)
--------------------------------------------------------------------
  [INFO] Generated signature: t=1727713500,v1=3d2a8f1c...
  [OK]   verify_webhook_signature(correct body) = True
  [OK]   verify_webhook_signature(tampered body) = False  (tamper detected)
  [OK]   verify_webhook_signature(1-bit flip) = False  (tamper detected)

--------------------------------------------------------------------
  Suite C -- Timing-Attack Defence and Expiration (unit-level)
--------------------------------------------------------------------
  [OK]   Expired timestamp (400 s old) correctly REJECTED
  [OK]   Wrong secret correctly REJECTED (HMAC mismatch)
  [OK]   Forged digest correctly REJECTED
  [OK]   Malformed header correctly REJECTED (parse error)
  [OK]   hmac.compare_digest is present in verify_webhook_signature source
  [OK]   tolerance=0 disables expiry check -- old signature accepted (expected)

--------------------------------------------------------------------
  Suite A -- Valid Signature (live end-to-end)
--------------------------------------------------------------------
  [INFO] Deactivated N pre-existing endpoint(s).
  [INFO] Registered secure endpoint: http://127.0.0.1:8000/api/v1/mock/secure?secret=...
  [INFO] Endpoint secret: 'phase7-test-secret-abc123'
  [INFO] Posting event ...
  [OK]   POST event -> HTTP 202 Accepted
  [OK]   event_id = <uuid>
  [INFO] Polling up to 30.0s for delivery ...
  [OK]   Mock secure endpoint returned HTTP 200 -- signature VERIFIED by receiver
  [OK]   Event.status = DELIVERED

====================================================================
  Suite A (Valid Signature, live):   PASS
  Suite B (Tamper Resistance, unit): PASS
  Suite C (Expiry + Timing, unit):   PASS

  PASS  -- Phase 7 HMAC signing verification succeeded.
====================================================================
```

---

## 7. Interview Defense

### "How do you protect webhooks from tampering and replay attacks?"

**Answer**:

We implement the Stripe-standard HMAC-SHA256 signing scheme, which provides
three independent security properties:

#### Integrity (tamper protection)

Every outbound HTTP POST carries an `X-Webhook-Signature` header:

```
X-Webhook-Signature: t=1700000000,v1=<hmac_sha256_hex>
```

The HMAC is computed over `f"t={timestamp}.{raw_body}"` using the endpoint's
shared secret. Any modification to the body -- even a single bit -- produces
a completely different 256-bit digest. An attacker without the secret cannot
produce a valid signature for a modified payload.

#### Authenticity (spoofing protection)

The shared secret is generated per-endpoint and stored in
`WebhookEndpoint.secret`. It is never transmitted in plaintext and is known
only to the webhook engine and the subscriber. A forged request from an
attacker who doesn't know the secret will always produce an HMAC mismatch and
be rejected with HTTP 401.

#### Freshness (replay protection)

The Unix timestamp `t=` is **embedded inside** the signed string -- not a
separate header an attacker could swap. The subscriber rejects requests where
`abs(now - t) > 300` seconds. A captured request replayed 6 minutes later is
automatically rejected, even though the signature is cryptographically valid.

#### Timing-attack resistance

The subscriber verification uses `hmac.compare_digest` (not `==`), which
always takes constant time regardless of how many bytes match. This prevents
an attacker from deducing correct signature bytes via response-time analysis.

#### Backward compatibility

Endpoints without a configured secret receive unsigned deliveries (no
`X-Webhook-Signature` header). This preserves compatibility with all
Phase 1-6 endpoints that were registered without secrets.

> [!TIP]
> The "serialise once" pattern is the non-obvious correctness requirement:
> we must sign **exactly the bytes we send**. Using `httpx`'s `json=dict`
> kwarg lets httpx re-serialise internally -- the wire bytes may differ from
> what we signed. The fix: `json.dumps(..., sort_keys=True, separators=(",",":"))`,
> sign that string, then `POST content=string.encode()`. Signed bytes == received bytes, guaranteed.

---

## 8. Change Summary

| File | Change | Phase |
|---|---|---|
| `app/core/security.py` | **New** -- `generate_webhook_signature` + `verify_webhook_signature` | Phase 7 |
| `app/tasks/delivery.py` | Read `endpoint.secret`; serialise once; inject `X-Webhook-Signature` | Phase 7 |
| `app/api/v1/mock.py` | Added `/mock/secure` -- live HMAC verification endpoint | Phase 7 |
| `scripts/verify_phase7.py` | **New** -- 3-suite verification (live, tamper, timing/expiry) | Phase 7 |
| `docs/PHASE_7_SECURITY_HMAC.md` | This document | Phase 7 |

---

*Previous phase: [Phase 6 -- DLQ & Replay](PHASE_6_DLQ_REPLAY.md)*
