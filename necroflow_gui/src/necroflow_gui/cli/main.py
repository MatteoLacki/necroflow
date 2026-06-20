from __future__ import annotations

import argparse

from necroflow_gui.app import serve


def main() -> None:
    parser = argparse.ArgumentParser(prog="necroflow-gui")
    subparsers = parser.add_subparsers(dest="command")
    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("target", nargs="?", help="module[:PIPELINES] or file.py[:PIPELINES]")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.command in (None, "serve"):
        serve(
            target=getattr(args, "target", None),
            host=getattr(args, "host", "127.0.0.1"),
            port=getattr(args, "port", 8000),
        )


if __name__ == "__main__":
    main()
