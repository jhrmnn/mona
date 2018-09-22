# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess

from .rules import Rule

from typing import Dict, Union


@Rule
def bash(stdin: bytes, inputs: Dict[str, Union[bytes, Path]]
         ) -> Dict[str, bytes]:
    inputs = {'STDIN': stdin, **inputs}
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        for filename, target in inputs.items():
            if isinstance(target, bytes):
                (root/filename).write_bytes(target)
            elif isinstance(target, Path):
                (root/filename).symlink_to(target)
        with (root/'STDIN').open() as fstdin, \
                (root/'STDOUT').open('w') as stdout, \
                (root/'STDERR').open('w') as stderr:
            subprocess.run(
                ['bash'],
                stdin=fstdin, stdout=stdout, stderr=stderr,
                cwd=root, check=True,
            )
        outputs = {}
        for path in root.glob('**/*'):
            relpath = path.relative_to(root)
            if str(relpath) not in inputs and path.is_file():
                outputs[str(relpath)] = path.read_bytes()
    return outputs
