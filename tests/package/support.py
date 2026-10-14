"""Locates the built add-on so tests run against the vendored lib/ that ships.

Tests exercise ``output/nats`` (produced by ``./build.sh``) rather than
``package/`` so that both the add-on code and its vendored dependencies are
checked on the interpreter running the tests. Override the location with the
``NATS_APP_DIR`` environment variable.
"""

import importlib
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
APP_DIR = os.path.abspath(
    os.environ.get("NATS_APP_DIR", os.path.join(REPO_ROOT, "output", "nats"))
)
BIN_DIR = os.path.join(APP_DIR, "bin")
LIB_DIR = os.path.join(APP_DIR, "lib")

if not os.path.isdir(LIB_DIR):
    raise RuntimeError(
        f"Built add-on not found at {APP_DIR}; run ./build.sh <version> first "
        "or set NATS_APP_DIR"
    )


def import_bin_module(name):
    """Imports a module from the built bin/ with the vendored lib/ on sys.path."""
    for path in (LIB_DIR, BIN_DIR):
        if path not in sys.path:
            sys.path.insert(0, path)
    return importlib.import_module(name)
