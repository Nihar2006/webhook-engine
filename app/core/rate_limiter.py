"""
app/core/rate_limiter.py
~~~~~~~~~~~~~~~~~~~~~~~~
Atomic per-endpoint rate limiting via Redis Lua scripts — Phase 8.

Architecture
------------
Rate limiting in a distributed delivery system has one hard requirement:
**atomicity**.  A naïve Python implementation would look like::

    count = redis.get(key)          # read
    if count < limit:               # check
        redis.incr(key)             # act

Between the read and the act, another Celery worker on a different process
(or machine) may also read the same stale count and both proceed past the
limit — the classic *check-then-act* race condition.  Under 10 concurrent
workers and a limit of 5, you can get 10 accepted requests instead of 5.

The fix is to move the read+check+act into a **Redis Lua script**.  Redis
executes Lua scripts atomically — the entire script runs to completion on the
Redis thread before any other command is processed.  No interleaving, no race.

Fixed-Window Algorithm
----------------------
We use a **fixed-window** counter rather than a sliding window because:

1. ``O(1)`` memory per endpoint — a single integer key, not a sorted set.
2. ``O(1)`` time complexity — INCR is O(1) vs. ZRANGEBYSCORE O(log N + M).
3. Sufficient for Phase 8's goal of burst protection; sliding window is a
   Phase 9+ enhancement if sub-second fairness is required.

The trade-off: a client can send ``2 × limit`` requests across a window
boundary (limit at end of window N, limit at start of window N+1).  For
webhook delivery engines this is acceptable — we are protecting receivers from
sustained overload, not preventing micro-bursts at millisecond precision.

Lua Script (executed atomically by Redis)
-----------------------------------------
::

    local key     = KEYS[1]           -- "rate_limit:{endpoint_id}"
    local limit   = tonumber(ARGV[1]) -- max requests allowed in window
    local window  = tonumber(ARGV[2]) -- window duration in seconds

    local current = redis.call('INCR', key)
    if current == 1 then
        -- First increment this window: anchor the expiry.
        -- Subsequent INCRs inherit this TTL.
        redis.call('EXPIRE', key, window)
    end
    if current > limit then
        return 0   -- REJECTED: caller must back off
    end
    return 1       -- ALLOWED: proceed with delivery

Why EXPIRE only on current == 1?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If we called EXPIRE on every INCR the TTL would reset on every request,
turning a 1-second window into "1 second after the last request" — effectively
rate-limiting can never trigger under sustained load.  Setting TTL only when
the key is freshly created (counter == 1) anchors the window at the first
request of each period.

Usage
-----
::

    from app.core.rate_limiter import is_endpoint_rate_limited
    import redis

    r = redis.Redis.from_url("redis://localhost:6379/0")
    limited, retry_after = is_endpoint_rate_limited(r, "ep-uuid", limit=5, window_seconds=2)
    if limited:
        # back off for retry_after seconds
        ...
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis as redis_module

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis Lua script — fixed-window rate limiter (atomic, O(1))
# ---------------------------------------------------------------------------

_LUA_RATE_LIMIT_SCRIPT = """
local key     = KEYS[1]
local limit   = tonumber(ARGV[1])
local window  = tonumber(ARGV[2])

local current = redis.call('INCR', key)
if current == 1 then
    redis.call('EXPIRE', key, window)
end
if current > limit then
    return 0
end
return 1
"""


class RateLimitExceeded(Exception):
    """Raised by delivery tasks when an endpoint's rate limit is exceeded.

    Treated as a deferral signal — the task will be retried after
    ``retry_after_seconds``.  It is NOT a delivery failure and must not
    count against the network-error retry budget.
    """


class RateLimiter:
    """Atomic per-endpoint rate limiter backed by a Redis Lua script.

    Parameters
    ----------
    redis_client:
        A connected ``redis.Redis`` (or ``redis.StrictRedis``) instance.
        The client is reused across all calls — callers should supply a
        module-level singleton to avoid connection churn.

    Example
    -------
    ::

        import redis
        from app.core.rate_limiter import RateLimiter

        _redis = redis.Redis.from_url("redis://localhost:6379/0")
        _limiter = RateLimiter(_redis)

        limited, retry_after = _limiter.is_rate_limited("ep-abc", limit=10, window_seconds=1)
    """

    def __init__(self, redis_client: "redis_module.Redis") -> None:  # type: ignore[type-arg]
        self._redis = redis_client
        # register_script compiles the Lua source once and returns a callable
        # Script object that sends the script via EVALSHA (cached on Redis side).
        self._script = redis_client.register_script(_LUA_RATE_LIMIT_SCRIPT)
        logger.debug("[RateLimiter] Lua script registered.")

    def is_rate_limited(
        self,
        endpoint_id: str,
        limit: int = 10,
        window_seconds: int = 1,
    ) -> tuple[bool, int]:
        """Check whether *endpoint_id* has exceeded its rate limit.

        Executes the Lua script atomically on Redis.  If the counter has
        exceeded *limit* within the current *window_seconds*, returns
        ``(True, window_seconds)`` so the caller knows how long to wait.

        Parameters
        ----------
        endpoint_id:
            String UUID of the ``WebhookEndpoint`` — used to namespace the
            Redis key: ``rate_limit:{endpoint_id}``.
        limit:
            Maximum number of requests allowed in the window.
        window_seconds:
            Duration of the fixed time window in seconds.

        Returns
        -------
        tuple[bool, int]
            ``(is_limited, retry_after_seconds)``

            * ``is_limited = True``  → caller must back off.
            * ``is_limited = False`` → caller may proceed.
            * ``retry_after_seconds`` is meaningful only when ``is_limited``
              is True; it equals ``window_seconds`` (the worst-case wait for
              the current window to expire).

        Notes
        -----
        The actual remaining TTL could be fetched with a second Redis call
        (TTL key), but that adds a round-trip and the difference is
        sub-second.  Returning ``window_seconds`` as a conservative upper
        bound keeps the implementation O(1) and a single round-trip.
        """
        key = f"rate_limit:{endpoint_id}"
        try:
            result = self._script(keys=[key], args=[limit, window_seconds])
            allowed = bool(result)
        except Exception as exc:
            # If Redis is unreachable, fail open (allow) to avoid blocking
            # all deliveries during a Redis hiccup.  Log prominently.
            logger.error(
                "[RateLimiter] Redis script execution failed for endpoint=%s: %s — failing OPEN",
                endpoint_id, exc,
            )
            return False, 0

        if not allowed:
            logger.debug(
                "[RateLimiter] endpoint=%s RATE LIMITED (limit=%d window=%ds)",
                endpoint_id, limit, window_seconds,
            )
            return True, window_seconds

        logger.debug(
            "[RateLimiter] endpoint=%s ALLOWED (limit=%d window=%ds)",
            endpoint_id, limit, window_seconds,
        )
        return False, 0


# ---------------------------------------------------------------------------
# Module-level convenience function
# ---------------------------------------------------------------------------

def is_endpoint_rate_limited(
    redis_client: "redis_module.Redis",  # type: ignore[type-arg]
    endpoint_id: str,
    limit: int = 10,
    window_seconds: int = 1,
) -> tuple[bool, int]:
    """Stateless convenience wrapper around :class:`RateLimiter`.

    Creates a one-shot ``RateLimiter`` and delegates immediately.  Use this
    for simple call sites that don't need to reuse the limiter instance.
    For hot paths (e.g., Celery tasks), prefer a module-level singleton
    ``RateLimiter`` to avoid re-registering the Lua script on every call.

    Parameters
    ----------
    redis_client:
        Connected ``redis.Redis`` instance.
    endpoint_id:
        String UUID of the target endpoint.
    limit:
        Max requests in the window.
    window_seconds:
        Window duration in seconds.

    Returns
    -------
    tuple[bool, int]
        ``(is_limited, retry_after_seconds)`` — see :meth:`RateLimiter.is_rate_limited`.
    """
    limiter = RateLimiter(redis_client)
    return limiter.is_rate_limited(endpoint_id, limit=limit, window_seconds=window_seconds)
