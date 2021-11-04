# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import os
import subprocess
from typing import Any, Callable, Optional, Tuple, TypeVar, Union
from typing_extensions import Protocol, runtime

from .sessions import Session

__version__ = '0.1.1'
__all__ = ['run_shell', 'run_process']

log = logging.getLogger(__name__)

_T = TypeVar('_T')
ProcessOutput = Union[bytes, Tuple[bytes, bytes]]


@runtime
class Scheduler(Protocol):
    def __call__(self, func: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        ...


def _scheduler() -> Optional[Scheduler]:
    scheduler = Session.active().storage.get('scheduler')
    if scheduler is None:
        return None
    assert isinstance(scheduler, Scheduler)
    return scheduler


def run_shell(cmd: str, ncores: int = None, **kwargs: Any) -> ProcessOutput:
    """Execute a command in a shell.

    Wrapper around :func:`subproecss.run` that handles errors and whose behavior
    can be modified by session plugins.

    :param str cmd: a shell command to be executed
    :param int ncores: number of cores that should be taken by the process
    :param kwargs: all other keyword arguments are passed to
                   :func:`~subprocess.run`.  :data:`~subprocess.PIPE` is passed
                   to `stdin` and `stdout` keyword arguments by default.

    Return the standard output as bytes if no error output was generated or a
    tuple of bytes containing standard and error outputs.
    """
    scheduler = _scheduler()
    assert 'shell' not in kwargs
    kwargs['shell'] = True
    if scheduler:
        return scheduler(_run_process, cmd, ncores=ncores, **kwargs)
    return _run_process(cmd, **kwargs)


def run_process(*args: str, ncores: int = None, **kwargs: Any) -> ProcessOutput:
    """Create a subprocess.

    Wrapper around :func:`subprocess.run` that handles errors and whose behavior
    can be modified by session plugins.

    :param str args: arguments of the subprocess
    :param int ncores: number of cores that should be taken by the process
    :param kwargs: all other keyword arguments are passed to
                   :func:`~subprocess.run`.  :data:`~subprocess.PIPE` is passed
                   to `stdin` and `stdout` keyword arguments by default.

    Return the standard output as bytes if no error output was generated or a
    tuple of bytes containing standard and error outputs.
    """
    scheduler = _scheduler()
    if scheduler:
        return scheduler(_run_process, args, ncores=ncores, **kwargs)
    return _run_process(args, **kwargs)


def _run_process(
    args: Union[str, Tuple[str, ...]],
    shell: bool = False,
    input: bytes = None,
    ncores: int = None,
    **kwargs: Any,
) -> Union[bytes, Tuple[bytes, bytes]]:
    kwargs.setdefault('stdin', subprocess.PIPE)
    kwargs.setdefault('stdout', subprocess.PIPE)
    kwargs.setdefault('env', os.environ.copy())
    if ncores is not None:
        kwargs['env']['MONA_NCORES'] = str(ncores)
    if shell:
        assert isinstance(args, str)
        proc = subprocess.Popen(args, shell=True, **kwargs)
    else:
        assert isinstance(args, tuple)
        proc = subprocess.Popen(args, **kwargs)
    stdout, stderr = proc.communicate(input)
    if proc.returncode:
        log.error(f'Got nonzero exit code in {args!r}')
        raise subprocess.CalledProcessError(proc.returncode, args)
    if stderr is None:
        return stdout  # type: ignore
    return stdout, stderr
