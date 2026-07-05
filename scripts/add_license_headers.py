"""One-off script: prepend the ASF license header to every .py file under
src/, dags/, autosys/, tests/ that doesn't already have one. Idempotent."""
from __future__ import annotations

from pathlib import Path

HEADER = '''# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the
# License.
'''

MARKER = "Licensed to the Apache Software Foundation"

ROOT = Path(__file__).parent.parent
TARGET_DIRS = ["src", "dags", "autosys", "tests"]


def main() -> None:
    changed = 0
    for target_dir in TARGET_DIRS:
        for path in (ROOT / target_dir).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            if MARKER in text:
                continue
            path.write_text(HEADER + "\n" + text, encoding="utf-8")
            changed += 1
            print(f"headered: {path.relative_to(ROOT)}")
    print(f"\n{changed} files updated")


if __name__ == "__main__":
    main()
