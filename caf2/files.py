# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import stat
from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess

from .rules import Rule

from typing import Dict, Union


def make_executable(path: str) -> None:
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


@Rule
def dir_task(script: bytes, inputs: Dict[str, Union[bytes, Path]]
             ) -> Dict[str, bytes]:
    inputs = {'SCRIPT': script, **inputs}
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        scriptfile = str(root/'SCRIPT')
        for filename, target in inputs.items():
            if isinstance(target, bytes):
                (root/filename).write_bytes(target)
            elif isinstance(target, Path):
                (root/filename).symlink_to(target)
        make_executable(scriptfile)
        with (root/'STDOUT').open('w') as stdout, \
                (root/'STDERR').open('w') as stderr:
            subprocess.run(
                [scriptfile], stdout=stdout, stderr=stderr, cwd=root, check=True,
            )
        outputs = {}
        for path in root.glob('**/*'):
            relpath = path.relative_to(root)
            if str(relpath) not in inputs and path.is_file():
                outputs[str(relpath)] = path.read_bytes()
    return outputs
