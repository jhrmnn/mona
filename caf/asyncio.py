# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
from asyncio import Future, AbstractEventLoop  # noqa
from typing import (
    Awaitable, TypeVar, List, Union, Generator, Any, Type, Dict
)

_T = TypeVar('_T')
_U = TypeVar('_U', bound=Exception)
_FutureT = Union['Future[_T]', Generator[Any, None, _T], Awaitable[_T]]


async def gather(*coros_or_futures: _FutureT[_T], loop: AbstractEventLoop = None,
                 returned_exception: Type[_U]) -> List[Union[_T, _U]]:
    results: Dict['Future[_T]', Union[_T, _U]] = {}
    futures = list(map(asyncio.ensure_future, coros_or_futures))
    pending = set(futures)
    while pending:
        try:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_EXCEPTION
            )
        except asyncio.CancelledError:
            for fut in futures:
                fut.cancel()
            raise
        for fut in done:
            try:
                results[fut] = fut.result()
            except returned_exception as e:
                results[fut] = e
            except Exception as e:
                for fut in futures:
                    fut.cancel()
                raise
    return [results[fut] for fut in futures]
