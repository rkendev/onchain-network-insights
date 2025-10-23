import re
import sys
from pathlib import Path

# Patterns for common secret shapes
PATTERNS = [
    re.compile(r"(?:api|secret|token|key)[^\n]{0,40}['\"][A-Za-z0-9_-]{16,}['\"]", re.IGNORECASE),
    re.compile(r"AIza[0-9A-Za-z\-_]{35}"),  # Google style key
    re.compile(r"(?i)aws(_|-)?access(_|-)?key(_|-)?id\s*[:=]\s*[A-Z0-9]{20}"),
    re.compile(r"(?i)aws(_|-)?secret(_|-)?access(_|-)?key\s*[:=]\s*[A-Za-z0-9/+=]{40}"),
    re.compile(r"(?i)secret\s*[:=]\s*[A-Za-z0-9/+=]{16,}"),
    re.compile(r"(?i)rpc(_|-)?url\s*[:=]\s*https?://[^\s]+"),
]

ALLOWLIST_EXT = {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".db"}

def file_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def main(paths):
    violations = []
    for path in paths:
        p = Path(path)
        if not p.exists() or p.suffix.lower() in ALLOWLIST_EXT:
            continue
        text = file_text(p)
        for pat in PATTERNS:
            for m in pat.finditer(text):
                frag = m.group(0)
                if "YOUR" in frag.upper() or "PLACEHOLDER" in frag.upper():
                    continue
                if "# secrets: allow" in text[max(0, m.start()-120):m.end()+120]:
                    continue
                violations.append((str(p), frag[:80]))
    if violations:
        print("Potential secrets detected:")
        for f, frag in violations:
            print(f" - {f}: {frag}")
        print("If these are false positives, add an inline comment: # secrets: allow")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main(sys.argv[1:])
