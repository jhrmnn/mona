# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sqlite3
from textwrap import dedent
import pickle

from .caf import Session, Task
from caf.Utils import get_timestamp

from typing import Callable, Any, Optional, Tuple, Set


def init_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute(dedent(
        """\
        CREATE TABLE IF NOT EXISTS tasks (
            hashid   TEXT,
            created  TEXT,
            result   BLOB,
            PRIMARY KEY (hashid)
        )
        """
    ))
    # db.execute(dedent(
    #     """\
    #     CREATE TABLE IF NOT EXISTS task_children (
    #         parent   TEXT,
    #         child    TEXT,
    #         FOREIGN KEY(parent) REFERENCES builds(hashid),
    #         FOREIGN KEY(child)  REFERENCES tasks(hashid)
    #     )
    #     """
    # ))
    return db


class CachedSession(Session):
    def __init__(self, db: sqlite3.Connection) -> None:
        super().__init__()
        self._db = db
        self._processed_tasks: Set[Task] = set()

    def create_task(self, f: Callable, *args: Any) -> Task:
        task = super().create_task(f, *args)
        if task in self._processed_tasks:
            return task
        row: Optional[Tuple[Optional[bytes]]] = self._db.execute(
            'SELECT result FROM tasks WHERE hashid = ?', (task.hashid,)
        ).fetchone()
        if not row:
            self._db.execute(
                'INSERT INTO tasks VALUES (?,?,?)',
                (task.hashid, get_timestamp(), None)
            )
            self._db.commit()
            task.add_done_callback(self._store_result)
        else:
            pickled_result, = row
            if pickled_result:
                task.set_result(pickle.loads(pickled_result))
        return task

    def _store_result(self, task: Task) -> None:
        self._db.execute(
            'UPDATE tasks SET result = ? WHERE hashid = ?',
            (pickle.dumps(task.result()), task.hashid)
        )
        self._db.commit()
