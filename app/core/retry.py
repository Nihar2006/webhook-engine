"""
app/core/retry.py
~~~~~~~~~~~~~~~~~
Pure-Python backoff utility — Phase 4.

Why Full Jitter?
----------------
Naive exponential backoff schedules retries at fixed multiples of a base
delay::

    sleep = base * 2 ** attempt   # 1 s, 2 s, 4 s, 8 s, 16 s …

This is better than a fixed delay, but it has a fatal flaw: when a large
number of workers all fail at the same instant (e.g. a brief network hiccup
at T=0), they all back off for exactly the same duration and **retry in
lockstep**.  The recovering service receives a coordinated "thundering herd"
of requests at T=2, then again at T=4, T=8 — potentially re-overwhelming it
on each wave.

Full Jitter breaks the synchronisation by choosing a random sleep uniformly
from the interval [0, cap]::

    cap   = min(max_delay, base * 2 ** attempt)   # exponential ceiling
    sleep = random.uniform(0, cap)                 # uniform spread

Key properties
~~~~~~~~~~~~~~
* **Expected value** ≈ cap / 2  — average wait grows with attempt number, so
  the system still backs off under sustained load.
* **Variance** = cap² / 12  — high variance means workers spread out across
  the full interval, flattening the concurrency spike on the recovering service.
* **Bounded** — sleep never exceeds ``max_delay``, preventing unbounded waits
  from accumulating in high-attempt scenarios.

Empirically (from the AWS Architecture Blog, "Exponential Backoff And Jitter",
2015), Full Jitter reduces completed-task time by ~50% compared with naive
exponential backoff under high concurrency, while the recovering server sees
a nearly flat request rate rather than synchronized spikes.
"""
from __future__ import annotations

import random


def calculate_full_jitter_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> float:
    """Return a Full-Jitter backoff delay in seconds for *attempt*.

    Parameters
    ----------
    attempt:
        Zero-indexed retry count (0 = first retry, 1 = second retry, …).
        Equivalent to ``self.request.retries`` inside a Celery task.
    base_delay:
        The base unit of delay in seconds (default 1 s).
    max_delay:
        Hard upper bound on the sleep duration in seconds (default 60 s).
        Prevents runaway waits at high attempt numbers.

    Returns
    -------
    float
        Seconds to wait before the next attempt.  Always in [0, max_delay].

    Algorithm
    ---------
    ::

        # Exponential ceiling — grows as 1, 2, 4, 8, … capped at max_delay.
        cap   = min(max_delay, base_delay * (2 ** attempt))

        # Uniform draw over [0, cap] — this is the "Full Jitter" step.
        # Workers that failed simultaneously now independently pick random
        # points across [0, cap], spreading retry load across time instead
        # of concentrating it at a single moment.
        sleep = random.uniform(0, cap)
    """
    # Exponential ceiling: grows geometrically but never exceeds max_delay.
    # Without the cap, 2^attempt overflows at attempt=1024+, causing
    # arbitrarily long waits that hurt liveness guarantees.
    cap: float = min(max_delay, base_delay * (2 ** attempt))

    # Full Jitter: uniform random draw across the entire [0, cap] window.
    # This is what distinguishes "Full Jitter" from "Equal Jitter" (which
    # uses cap/2 + uniform(0, cap/2)) or "Decorrelated Jitter".
    # Full Jitter has the highest variance and the best thundering-herd
    # suppression of the three variants.
    return random.uniform(0, cap)
