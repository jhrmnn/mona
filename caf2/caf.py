# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import subprocess
from typing import Any


async def run_process(*args: str, **kwargs: Any) -> None:
    proc = await asyncio.create_subprocess_exec(*args, **kwargs)
    retcode = await proc.wait()
    if retcode:
        raise subprocess.CalledProcessError(retcode, args)
