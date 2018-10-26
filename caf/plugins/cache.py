# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import sqlite3
import pickle
from enum import Enum
from itertools import chain
from textwrap import dedent

from ..sessions import SessionPlugin
from ..futures import State
from ..tasks import Task
from ..utils import Pathable, get_timestamp, get_fullname, import_fullname
from ..hashing import Hash, Hashed

from typing import Any, Optional, Set, TypeVar, NamedTuple, Union, \
    Iterable, Dict, cast, Type, Deque

log = logging.getLogger(__name__)



class TaskRow(NamedTuple):
    hashid: Hash
    label: str
    state: str
    created: str
    side_effects: Optional[str] = None
    result_type: Optional[str] = None
    result: Union[None, Hash, bytes] = None


class ObjectRow(NamedTuple):
    hashid: Hash
    typetag: str
    spec: bytes


class ResultType(Enum):
    HASHED = 0
    PICKLED = 1


class Cache(SessionPlugin):
    name = 'db_cache'

    def __init__(self, db: sqlite3.Connection, eager: bool = True) -> None:
        self._db = db
        self._pending: Set[Hash] = set()
        self._objects: Dict[Hash, Hashed[object]] = {}
        self._eager = eager
        self._to_restore: Optional[List[Task[object]]] = None

    def __repr__(self) -> str:
        return (
            f'<Cache npending={len(self._pending)} '
            f'nobjects={len(self._objects)}>'
        )

    @property
    def db(self) -> sqlite3.Connection:
        return self._db

    def _store_objects(self, objs: Iterable[Hashed[object]]) -> None:
        obj_rows = [ObjectRow(
            hashid=obj.hashid,
            typetag=get_fullname(obj.__class__),
            spec=obj.spec
        ) for obj in objs]
        self._db.executemany(
            'INSERT OR IGNORE INTO objects VALUES (?,?,?)', obj_rows
        )

    def _store_tasks(self, tasks: Iterable[Task[object]]) -> None:
        task_rows = [TaskRow(
            hashid=task.hashid,
            label=task.label,
            state=task.state.name,
            created=get_timestamp(),
        ) for task in tasks]
        self._db.executemany(
            'INSERT INTO tasks VALUES (?,?,?,?,?,?,?)', task_rows
        )

    def _store_result(self, task: Task[object]) -> None:
        result: Union[Hash, bytes]
        hashed: Hashed[object]
        if task.state is State.AWAITING:
            hashed = task.future_result()
            result_type = ResultType.HASHED
        else:
            assert task.state is State.DONE
            hashed_or_obj = task.resolve()
            if isinstance(hashed_or_obj, Hashed):
                result_type = ResultType.HASHED
                hashed = hashed_or_obj
            else:
                result_type = ResultType.PICKLED
                result = pickle.dumps(hashed_or_obj)
        if result_type is ResultType.HASHED:
            result = hashed.hashid
        side_effects = ','.join(
            t.hashid for t in self._app.get_side_effects(task)
        )
        self._db.execute(
            'UPDATE tasks SET side_effects = ?, result_type = ?, '
            'result = ?, state = ? WHERE hashid = ?',
            (side_effects, result_type.name, result, task.state.name, task.hashid)
        )

    def _get_object(self, hashid: Hash) -> Hashed[Any]:
        raw_row = self._db.execute(
            'SELECT * FROM objects WHERE hashid = ?', (hashid,)
        ).fetchone()
        assert raw_row
        row = ObjectRow(*raw_row)
        factory = cast(Type[Hashed[Any]], import_fullname(row.typetag))
        assert issubclass(factory, Hashed)
        hashed = cast(Hashed[Any], factory.from_spec(row.spec, self._get_object))
        if isinstance(hashed, Task):
            hashed = self._app.process_task(hashed)
        return hashed

    def save_hashed(self, objs: Iterable[Hashed[object]]) -> None:
        if self._to_restore is not None:
            return
        if self._eager:
            self._store_objects(objs)
            self._db.commit()
        else:
            self._objects.update({o.hashid: o for o in objs})

    def post_create(self, task: Task[object]) -> None:
        if self._to_restore is not None:
            self._to_restore.append(task)
            return
        self._to_restore = Deque([task])
        while self._to_restore:
            task = self._to_restore.popleft()
            self._post_create(task)
        self._to_restore = None

    def _post_create(self, task: Task[_T]) -> None:
        raw_row = self._db.execute(
            'SELECT * FROM tasks WHERE hashid = ?', (task.hashid,)
        ).fetchone()
        if not raw_row:
            assert not self._to_restore
            if self._eager:
                self._store_tasks([task])
                self._store_objects([task])
                self._db.commit()
            else:
                self._pending.add(task.hashid)
            return
        row = TaskRow(*raw_row)
        if State[row.state] < State.HAS_RUN:
            return
        task._label = row.label
        log.info(f'Restoring from cache: {task}')
        assert row.result_type
        task.set_running()
        if row.side_effects:
            with self._app.running_task_ctx(task):
                for hashid in row.side_effects.split(','):
                    child_task = self._get_object(cast(Hash, hashid))
                    assert isinstance(child_task, Task)
        task.set_has_run()
        result_type = ResultType[row.result_type]
        if result_type is ResultType.PICKLED:
            assert isinstance(row.result, bytes)
            result = pickle.loads(row.result)
        else:
            assert result_type is ResultType.HASHED
            assert isinstance(row.result, str)
            result = self._get_object(cast(Hash, row.result))
        self._app.set_result(task, result)

    def update_state(self, task: Task[_T]) -> None:
        self._db.execute(
            'UPDATE tasks SET state = ? WHERE hashid = ?',
            (task.state.name, task.hashid)
        )

    def post_task_run(self, task: Task[object]) -> None:
        if not self._eager:
            return
        self._store_result(task)
        if task.state < State.DONE:
            task.add_done_callback(lambda task: self.update_state(task))
        self._db.commit()

    def store_pending(self) -> None:
        tasks = [self._app.get_task(hashid) for hashid in self._pending]
        self._store_tasks(tasks)
        for task in tasks:
            if task.state > State.HAS_RUN:
                self._store_result(task)
        self._store_objects(chain(self._objects.values(), tasks))
        self._pending.clear()
        self._objects.clear()
        self._db.commit()

    @classmethod
    def from_path(cls, path: Pathable, **kwargs: Any) -> 'Cache':
        db = sqlite3.connect(path)
        db.execute(dedent(
            """\
            CREATE TABLE IF NOT EXISTS tasks (
                hashid         TEXT,
                label          TEXT,
                state          TEXT,
                created        TEXT,
                side_effects   TEXT,
                result_type    TEXT,
                result         BLOB,
                PRIMARY KEY (hashid)
            )
            """
        ))
        db.execute(dedent(
            """\
            CREATE TABLE IF NOT EXISTS objects (
                hashid   TEXT,
                typetag  TEXT,
                spec     BLOB,
                PRIMARY KEY (hashid)
            )
            """
        ))
        return Cache(db, **kwargs)
