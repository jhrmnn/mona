# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sqlite3
from textwrap import dedent
import pickle

from ..sessions import SessionPlugin
from ..tasks import Task
from ..utils import Pathable
from caf.Utils import get_timestamp

from typing import Any, Optional, Tuple, Set, TypeVar

_T = TypeVar('_T')


class Cache(SessionPlugin):
    name = 'db_cache'

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db
        self._processed_tasks: Set[Task[Any]] = set()

    @property
    def db(self) -> sqlite3.Connection:
        return self._db

    def post_create(self, task: Task[_T]) -> Task[_T]:
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
                task.set_running()
                task.set_has_run()
                task.set_result(pickle.loads(pickled_result))
        return task

    def _store_result(self, task: Task[Any]) -> None:
        self._db.execute(
            'UPDATE tasks SET result = ? WHERE taskid = ?',
            (pickle.dumps(task.value), task.hashid)
        )
        self._db.commit()

    @classmethod
    def from_path(cls, path: Pathable) -> 'Cache':
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
        return Cache(db)
