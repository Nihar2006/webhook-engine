"""Fix psycopg2 UUID adaptation issue in benchmark_async.py."""
path = "scripts/benchmark_async.py"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Fix 1: count_delivery_attempts — use ::uuid cast, pass strings
old1 = (
    '        placeholders = ",".join(["%s"] * len(event_ids))\n'
    '        cur.execute(\n'
    '            f"SELECT COUNT(*) FROM delivery_attempt WHERE event_id IN ({placeholders})",\n'
    '            [uuid.UUID(eid) for eid in event_ids],\n'
    '        )'
)
new1 = (
    '        placeholders = ",".join(["%s::uuid"] * len(event_ids))\n'
    '        cur.execute(\n'
    '            f"SELECT COUNT(*) FROM delivery_attempt WHERE event_id IN ({placeholders})",\n'
    '            event_ids,\n'
    '        )'
)

# Fix 2: get_event_statuses
old2 = (
    '        placeholders = ",".join(["%s"] * len(event_ids))\n'
    '        cur.execute(\n'
    '            f"SELECT id::text, status FROM event WHERE id IN ({placeholders})",\n'
    '            [uuid.UUID(eid) for eid in event_ids],\n'
    '        )'
)
new2 = (
    '        placeholders = ",".join(["%s::uuid"] * len(event_ids))\n'
    '        cur.execute(\n'
    '            f"SELECT id::text, status FROM event WHERE id IN ({placeholders})",\n'
    '            event_ids,\n'
    '        )'
)

if old1 in content:
    content = content.replace(old1, new1)
    print("Fixed count_delivery_attempts")
else:
    print("WARN: count pattern not found - printing lines for inspection")
    for i, line in enumerate(content.splitlines(), 1):
        if "delivery_attempt" in line:
            print(f"  {i}: {repr(line)}")

if old2 in content:
    content = content.replace(old2, new2)
    print("Fixed get_event_statuses")
else:
    print("WARN: event statuses pattern not found")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
print("Saved.")
