#!/usr/bin/env python3
"""Remove fmt stdext::checked_array_iterator usage for Visual Studio 2026."""

from __future__ import annotations

import sys
from pathlib import Path

FMT_SECURE_SCL_BLOCK = """#ifdef _SECURE_SCL
// Make a checked iterator to avoid MSVC warnings.
template <typename T> using checked_ptr = stdext::checked_array_iterator<T*>;
template <typename T> checked_ptr<T> make_checked(T* p, std::size_t size) {
  return {p, size};
}
#else
template <typename T> using checked_ptr = T*;
template <typename T> inline T* make_checked(T* p, std::size_t) { return p; }
#endif"""

FMT_SECURE_SCL_REPLACEMENT = """template <typename T> using checked_ptr = T*;
template <typename T> inline T* make_checked(T* p, std::size_t) { return p; }"""


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <path/to/fmt/format.h>", file=sys.stderr)
        return 2

    path = Path(sys.argv[1])
    text = path.read_text(encoding="utf-8")
    if FMT_SECURE_SCL_BLOCK not in text:
        if FMT_SECURE_SCL_REPLACEMENT in text:
            return 0
        print(f"fmt patch pattern not found in {path}", file=sys.stderr)
        return 1

    path.write_text(text.replace(FMT_SECURE_SCL_BLOCK, FMT_SECURE_SCL_REPLACEMENT), encoding="utf-8")
    print(f"Patched {path} for Visual Studio 2026")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
