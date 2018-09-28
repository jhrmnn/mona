# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import subprocess
from typing import Any, TypeVar, Callable, Awaitable, Optional, Tuple, Union
from typing_extensions import Protocol, runtime

from .sessions import Session

_T = TypeVar('_T')
ProcessOutput = Union[bytes, Tuple[bytes, bytes]]


@runtime
class Scheduler(Protocol):
    async def __call__(self,
                       corofunc: Callable[..., Awaitable[_T]],
                       *args: Any,
                       **kwargs: Any) -> _T: ...


def _scheduler() -> Optional[Scheduler]:
    scheduler = Session.active().storage.get('scheduler')
    if scheduler is None:
        return None
    assert isinstance(scheduler, Scheduler)
    return scheduler


async def run_shell(cmd: str, **kwargs: Any) -> ProcessOutput:
    scheduler = _scheduler()
    assert 'shell' not in kwargs
    kwargs['shell'] = True
    if scheduler:
        return await scheduler(_run_process, cmd, **kwargs)
    return await _run_process(cmd, **kwargs)


async def run_process(*args: str, **kwargs: Any) -> ProcessOutput:
    scheduler = _scheduler()
    if scheduler:
        return await scheduler(_run_process, args, **kwargs)
    return await _run_process(args, **kwargs)


async def _run_process(args: Union[str, Tuple[str, ...]],
                       shell: bool = False,
                       input: bytes = None,
                       **kwargs: Any) -> Union[bytes, Tuple[bytes, bytes]]:
    kwargs.setdefault('stdin', subprocess.PIPE)
    kwargs.setdefault('stdout', subprocess.PIPE)
    if shell:
        assert isinstance(args, str)
        proc = await asyncio.create_subprocess_shell(args, **kwargs)
    else:
        assert isinstance(args, tuple)
        proc = await asyncio.create_subprocess_exec(*args, **kwargs)
    stdout, stderr = await proc.communicate(input)
    if proc.returncode:
        raise subprocess.CalledProcessError(proc.returncode, args)
    if stderr is None:
        return stdout
    return stdout, stderr


async def run_thread(func: Callable[..., _T], *args: Any) -> _T:
    scheduler = _scheduler()
    if scheduler:
        return await scheduler(_run_thread, func, *args)
    return await _run_thread(func, *args)


async def _run_thread(func: Callable[..., _T], *args: Any) -> _T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)
