"""Every module the add-on ships in bin/ must import on each supported Python.

Splunk runs the modular inputs, search commands and REST handlers on whichever
of 3.9 / 3.13 the deployment selects, so a module that only imports on newer
Python (e.g. PEP 604 ``X | None`` annotations) breaks the add-on on 3.9.
"""

import os
import subprocess
import sys
import unittest

from support import BIN_DIR, LIB_DIR

# Each module imports in a fresh isolated interpreter: the UCC-generated
# import_declare_test rewrites sys.path, and Splunk loads every script alone.
IMPORT_SNIPPET = "import sys; sys.path[:0] = [sys.argv[1], sys.argv[2]]; import {name}"


# UCC-generated REST handlers import Splunk's own ``splunk`` package, which only
# exists inside splunkd; tests/e2e/search_commands_python.sh imports them there.
SPLUNK_ONLY_PREFIX = "nats_rh_"


def bin_modules():
    return sorted(
        name[: -len(".py")]
        for name in os.listdir(BIN_DIR)
        if name.endswith(".py")
        and not name.startswith(".")
        and not name.startswith(SPLUNK_ONLY_PREFIX)
    )


class BinImportTest(unittest.TestCase):
    def test_bin_has_expected_modules(self):
        modules = bin_modules()
        for expected in (
            "nats_kv_command",
            "nats_kv_helper",
            "nats_subscribe_command",
            "nats_subscribe_helper",
            "natskv",
            "natssubscribe",
            "nats_kv",
            "nats_subscribe",
        ):
            self.assertIn(expected, modules)

    def test_every_bin_module_imports(self):
        for name in bin_modules():
            with self.subTest(module=name):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-I",
                        "-c",
                        IMPORT_SNIPPET.format(name=name),
                        BIN_DIR,
                        LIB_DIR,
                    ],
                    cwd=BIN_DIR,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=120,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    f"import {name} failed on Python {sys.version.split()[0]}:\n"
                    f"{result.stderr}",
                )


if __name__ == "__main__":
    unittest.main()
