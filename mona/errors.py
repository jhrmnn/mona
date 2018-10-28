# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Any, TYPE_CHECKING  # noqa

if TYPE_CHECKING:
    from .futures import Future  # noqa
    from .tasks import Task  # noqa
    from .sessions import Session  # noqa

__version__ = '0.1.0'


class MonaError(Exception):
    pass


class FutureError(MonaError):
    def __init__(self, msg: str, fut: 'Future') -> None:
        super().__init__(msg)
        self.future = fut


class TaskError(MonaError):
    def __init__(self, msg: str, task: 'Task[Any]') -> None:
        super().__init__(msg)
        self.task = task


class CompositeError(MonaError):
    pass


class SessionError(MonaError):
    def __init__(self, msg: str, sess: 'Session') -> None:
        super().__init__(msg)
        self.session = sess


class InvalidInput(MonaError):
    pass


class FilesError(MonaError):
    pass


class HashingError(MonaError):
    pass
