"""Fix all non-ASCII characters in benchmark_async.py to be Windows CP1252 safe."""
path = "scripts/benchmark_async.py"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Encode to ASCII replacing anything that can't be represented
fixed = content.encode("ascii", "replace").decode("ascii")

# Clean up the replacement artifact characters
fixed = fixed.replace("?", "-", )  # only replace ? that came from box-drawing

# Actually do a targeted replace of specific known Unicode chars
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

replacements = [
    ("\u2014", "--"),    # em dash
    ("\u2013", "-"),     # en dash
    ("\u2500", "-"),     # box drawing dash
    ("\u2192", "->"),    # right arrow
    ("\u2265", ">="),    # >=
    ("\u2264", "<="),    # <=
    ("\u00d7", "x"),     # multiplication sign
    ("\u2713", "[OK]"),  # check mark
    ("\u2717", "[X]"),   # cross
    ("\u274c", "[X]"),   # red X
    ("\u2728", "**"),    # sparkles
    ("\u2019", "'"),     # right single quote
    ("\u2018", "'"),     # left single quote
    ("\u201c", '"'),     # left double quote
    ("\u201d", '"'),     # right double quote
    ("\u2026", "..."),   # ellipsis
    ("\u00b7", "."),     # middle dot
]

fixed = content
for char, repl in replacements:
    fixed = fixed.replace(char, repl)

# Final safety pass: replace any remaining non-ASCII with '?'
fixed_safe = fixed.encode("ascii", "replace").decode("ascii")

# Count replacements
remaining = sum(1 for c in fixed if ord(c) > 127)
print(f"Non-ASCII remaining before final pass: {remaining}")

with open(path, "w", encoding="utf-8") as f:
    f.write(fixed_safe)

print("Written successfully. All non-ASCII replaced.")
