import os
import sys

# The add-on modules live in package/bin; their dependencies (nats-py, solnlib,
# splunk-sdk) come from tests/requirements.txt instead of the vendored lib/.
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "package", "bin")
)
