"""Fix benchmark poll_for_delivery to reconnect psycopg2 on TCP abort errors."""
path = "scripts/benchmark_async.py"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

old = '''def poll_for_delivery(event_ids: list[str]) -> tuple[int, float]:
    """
    Poll the DB until all expected DeliveryAttempt rows are created
    (or timeout). Each event x each endpoint = one attempt row.

    Returns
    -------
    total_attempts : int
    elapsed_s      : float   -- seconds waited
    """
    expected = len(event_ids) * NUM_ENDPOINTS
    conn = get_sync_db_conn()
    conn.autocommit = True

    print(f"\\n  Polling DB for {expected} DeliveryAttempt rows "
          f"(timeout={DB_POLL_TIMEOUT_S:.0f} s)...")

    t0 = time.perf_counter()
    last_count = -1
    while True:
        count = count_delivery_attempts(conn, event_ids)
        elapsed = time.perf_counter() - t0

        if count != last_count:
            print(f"    [{elapsed:5.1f} s]  DeliveryAttempts found: {count}/{expected}")
            last_count = count

        if count >= expected:
            conn.close()
            return count, elapsed

        if elapsed >= DB_POLL_TIMEOUT_S:
            statuses = get_event_statuses(conn, event_ids)
            conn.close()
            print(f"\\n  [WARN] Timeout after {elapsed:.1f} s -- "
                  f"only {count}/{expected} attempts recorded.")
            print(f"  Event statuses: {statuses}")
            return count, elapsed

        time.sleep(DB_POLL_INTERVAL_S)'''

new = '''def poll_for_delivery(event_ids: list[str]) -> tuple[int, float]:
    """
    Poll the DB until all expected DeliveryAttempt rows are created
    (or timeout). Each event x each endpoint = one attempt row.

    Reconnects psycopg2 on Windows TCP connection aborts (10053) which
    can happen when Postgres is busy with concurrent writes.

    Returns
    -------
    total_attempts : int
    elapsed_s      : float   -- seconds waited
    """
    expected = len(event_ids) * NUM_ENDPOINTS
    print(f"\\n  Polling DB for {expected} DeliveryAttempt rows "
          f"(timeout={DB_POLL_TIMEOUT_S:.0f} s)...")

    # Give the worker a head-start before first poll
    time.sleep(2.0)

    def fresh_conn():
        c = get_sync_db_conn()
        c.autocommit = True
        return c

    conn = fresh_conn()
    t0 = time.perf_counter()
    last_count = -1

    while True:
        elapsed = time.perf_counter() - t0
        try:
            count = count_delivery_attempts(conn, event_ids)
        except Exception as db_err:
            # Reconnect on any connection error (Windows 10053, etc.)
            print(f"    [{elapsed:5.1f} s]  DB poll error ({type(db_err).__name__}), reconnecting...")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(1.0)
            try:
                conn = fresh_conn()
            except Exception:
                pass
            continue

        if count != last_count:
            print(f"    [{elapsed:5.1f} s]  DeliveryAttempts found: {count}/{expected}")
            last_count = count

        if count >= expected:
            try:
                conn.close()
            except Exception:
                pass
            return count, elapsed

        if elapsed >= DB_POLL_TIMEOUT_S:
            try:
                statuses = get_event_statuses(conn, event_ids)
                conn.close()
            except Exception:
                statuses = {}
            print(f"\\n  [WARN] Timeout after {elapsed:.1f} s -- "
                  f"only {count}/{expected} attempts recorded.")
            print(f"  Event statuses: {statuses}")
            return count, elapsed

        time.sleep(DB_POLL_INTERVAL_S)'''

if old in content:
    content = content.replace(old, new)
    print("Fixed poll_for_delivery")
else:
    print("ERROR: pattern not found")
    # Show the actual function for debugging
    start = content.find("def poll_for_delivery")
    print(repr(content[start:start+200]))

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
print("Saved.")
