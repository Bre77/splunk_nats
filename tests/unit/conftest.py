import os
import sys

# The add-on modules live in package/bin; their dependencies (nats-py, solnlib,
# splunk-sdk) come from tests/requirements.txt instead of the vendored lib/.
# Keep __pycache__ out of package/bin, which ucc-gen copies into the add-on.
sys.dont_write_bytecode = True
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "package", "bin")
)
