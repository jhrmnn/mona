# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sqlite3
from textwrap import dedent
import pickle

from .sessions import Session
from .tasks import Task
from .utils import Empty, Maybe
from caf.Utils import get_timestamp

from typing import Callable, Any, Optional, Tuple, Set, TypeVar

_T = TypeVar('_T')


def init_cafdb(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute(dedent(
        """\
        CREATE TABLE IF NOT EXISTS tasks (
            taskid   TEXT,
            label    TEXT,
            created  TEXT,
            result   BLOB,
            PRIMARY KEY (taskid)
        )
        """
    ))
    # db.execute(dedent(
    #     """\
    #     CREATE TABLE IF NOT EXISTS task_children (
    #         parent   TEXT,
    #         child    TEXT,
    #         FOREIGN KEY(parent) REFERENCES builds(taskid),
    #         FOREIGN KEY(child)  REFERENCES tasks(taskid)
    #     )
    #     """
    # ))
    return db


class CachedSession(Session):
    def __init__(self, db: sqlite3.Connection) -> None:
        Session.__init__(self)
        self._db = db
        self._processed_tasks: Set[Task[Any]] = set()

    def create_task(self, coro: Callable[..., _T], *args: Any,
                    label: str = None, default: Maybe[_T] = Empty._
                    ) -> Task[_T]:
        task = super().create_task(coro, *args, label=label, default=default)
        if task in self._processed_tasks:
            return task
        row: Optional[Tuple[Optional[bytes]]] = self._db.execute(
            'SELECT result FROM tasks WHERE taskid = ?', (task.hashid,)
        ).fetchone()
        if not row:
            self._db.execute(
                'INSERT INTO tasks VALUES (?,?,?,?)',
                (task.hashid, task.label, get_timestamp(), None)
            )
            self._db.commit()
            task.add_done_callback(self._store_result)
        else:
            pickled_result, = row
            if pickled_result:
                task.set_result(pickle.loads(pickled_result))
        return task

    def _store_result(self, task: Task[Any]) -> None:
        self._db.execute(
            'UPDATE tasks SET result = ? WHERE taskid = ?',
            (pickle.dumps(task.value), task.hashid)
        )
        self._db.commit()
