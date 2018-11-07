# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import logging
import subprocess
from typing import Any, Callable, Optional, Tuple, TypeVar, Union
from typing_extensions import Protocol, runtime

from .sessions import Session
from .tasks import Corofunc

__version__ = '0.1.0'
__all__ = ['run_shell', 'run_process', 'run_thread']

log = logging.getLogger(__name__)

_T = TypeVar('_T')
ProcessOutput = Union[bytes, Tuple[bytes, bytes]]


@runtime
class Scheduler(Protocol):
    async def __call__(self, corofunc: Corofunc[_T], *args: Any, **kwargs: Any) -> _T:
        ...


def _scheduler() -> Optional[Scheduler]:
    scheduler = Session.active().storage.get('scheduler')
    if scheduler is None:
        return None
    assert isinstance(scheduler, Scheduler)
    return scheduler


async def run_shell(cmd: str, **kwargs: Any) -> ProcessOutput:
    """Execute a command in a shell.

    Wrapper around :func:`asyncio.create_subprocess_shell` that handles errors
    and whose behavior can be modified by session plugins.

    :param str cmd: a shell command to be executed
    :param kwargs: all keyword arguments are passed to
                   :func:`~asyncio.create_subprocess_shell`.
                   :data:`~subprocess.PIPE` is passed to `stdin` and `stdout`
                   keyword arguments by default.

    Return the standard output as bytes if no error output was generated or a
    tuple of bytes containing standard and error outputs.
    """
    scheduler = _scheduler()
    assert 'shell' not in kwargs
    kwargs['shell'] = True
    if scheduler:
        return await scheduler(_run_process, cmd, **kwargs)
    return await _run_process(cmd, **kwargs)


async def run_process(*args: str, **kwargs: Any) -> ProcessOutput:
    """Create a subprocess.

    Wrapper around :func:`asyncio.create_subprocess_exec` that handles errors
    and whose behavior can be modified by session plugins.

    :param str args: arguments of the subprocess
    :param kwargs: all keyword arguments are passed to
                   :func:`~asyncio.create_subprocess_exec`.
                   :data:`~subprocess.PIPE` is passed to `stdin` and `stdout`
                   keyword arguments by default.

    Return the standard output as bytes if no error output was generated or a
    tuple of bytes containing standard and error outputs.
    """
    scheduler = _scheduler()
    if scheduler:
        return await scheduler(_run_process, args, **kwargs)
    return await _run_process(args, **kwargs)


async def _run_process(
    args: Union[str, Tuple[str, ...]],
    shell: bool = False,
    input: bytes = None,
    **kwargs: Any,
) -> Union[bytes, Tuple[bytes, bytes]]:
    kwargs.setdefault('stdin', subprocess.PIPE)
    kwargs.setdefault('stdout', subprocess.PIPE)
    if shell:
        assert isinstance(args, str)
        proc = await asyncio.create_subprocess_shell(args, **kwargs)
    else:
        assert isinstance(args, tuple)
        proc = await asyncio.create_subprocess_exec(*args, **kwargs)
    try:
        stdout, stderr = await proc.communicate(input)
    except asyncio.CancelledError:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        else:
            await proc.wait()
        raise
    if proc.returncode:
        log.error(f'Got nonzero exit code in {args!r}')
        raise subprocess.CalledProcessError(proc.returncode, args)
    if stderr is None:
        return stdout
    return stdout, stderr


async def run_thread(func: Callable[..., _T], *args: Any) -> _T:
    """Run a callable in a new thread.

    Wrapper around :meth:`asyncio.AbstractEventLoop.run_in_executor` whose
    behavior can be modified by session plugins.

    :param func: a callable
    :param args: positional arguments to the callable.

    Return the result of the callable.
    """
    scheduler = _scheduler()
    if scheduler:
        return await scheduler(_run_thread, func, *args)
    return await _run_thread(func, *args)


async def _run_thread(func: Callable[..., _T], *args: Any) -> _T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)
