# Copyright 2025 Brett Adams
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ucc-gen build hook: called with the output directory after the build."""

import glob
import os


def cleanup_output_files(output_path: str, ta_name: str) -> None:
    # ecdsa ships its own test suite inside the package; it is dead weight in the
    # add-on and its subprocess use triggers AppInspect threading notices.
    tests = os.path.join(output_path, ta_name, "lib", "ecdsa", "test_*.py")
    for path in glob.glob(tests):
        os.remove(path)
