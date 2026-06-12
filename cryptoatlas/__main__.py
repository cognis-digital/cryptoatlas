import sys

from cryptoatlas.cli import main

if __name__ == "__main__":
    # Token/entity names can contain non-ASCII; force UTF-8 stdout/stderr so the
    # CLI never crashes on a Windows cp1252 console.
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):  # pragma: no cover - older streams
            pass
    raise SystemExit(main())
