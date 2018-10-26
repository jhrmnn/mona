# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import sqlite3
import pickle
from enum import Enum
from itertools import chain
from textwrap import dedent
from weakref import WeakValueDictionary

from ..sessions import SessionPlugin
from ..futures import State, Future
from ..tasks import Task
from ..utils import Pathable, get_timestamp, get_fullname, \
    import_fullname
from ..hashing import Hash, Hashed

from typing import Any, Optional, Set, NamedTuple, Union, \
    Iterable, Dict, cast, Type, Tuple, TypeVar, List

log = logging.getLogger(__name__)

_T_co = TypeVar('_T_co', covariant=True)


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


class CachedTask(Task[_T_co]):
    def __init__(self, hashid: Hash, label: str) -> None:
        self._hashid = hashid
        self._args = ()
        Future.__init__(self, [])  # type: ignore
        self._label = label
        self._hook = None


class Cache(SessionPlugin):
    name = 'db_cache'

    def __init__(self, db: sqlite3.Connection, eager: bool = True,
                 restore_tasks: bool = False) -> None:
        self._db = db
        self._pending: Set[Hash] = set()
        self._objects: Dict[Hash, Hashed[object]] = {}
        self._eager = eager
        self._to_restore: Optional[List[Task[object]]] = None
        self._restore_tasks = restore_tasks
        self._object_cache: 'WeakValueDictionary[Hash, Hashed[object]]' \
            = WeakValueDictionary()

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

    def _update_state(self, task: Task[object]) -> None:
        self._db.execute(
            'UPDATE tasks SET state = ? WHERE hashid = ?',
            (task.state.name, task.hashid)
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
            'result = ? WHERE hashid = ?',
            (side_effects, result_type.name, result, task.hashid)
        )

    def _get_task_row(self, hashid: Hash) -> Optional[TaskRow]:
        raw_row = self._db.execute(
            'SELECT * FROM tasks WHERE hashid = ?', (hashid,)
        ).fetchone()
        if not raw_row:
            return None
        return TaskRow(*raw_row)

    def _get_object_factory(self, hashid: Hash
                            ) -> Tuple[bytes, Type[Hashed[object]]]:
        raw_row = self._db.execute(
            'SELECT * FROM objects WHERE hashid = ?', (hashid,)
        ).fetchone()
        assert raw_row
        row = ObjectRow(*raw_row)
        factory = import_fullname(row.typetag)
        assert issubclass(factory, Hashed)
        return row.spec, factory

    def _get_object(self, hashid: Hash) -> Hashed[object]:
        obj: Optional[Hashed[object]] = self._object_cache.get(hashid)
        if obj:
            return obj
        spec, factory = self._get_object_factory(hashid)
        if factory is Task and not self._restore_tasks:
            task_row = self._get_task_row(hashid)
            assert task_row
            if State[task_row.state] > State.HAS_RUN:
                obj = CachedTask(hashid, task_row.label)
        if not obj:
            obj = factory.from_spec(spec, self._get_object)
        if isinstance(obj, Task):
            obj = self._app.process_task(obj)
        self._object_cache[hashid] = obj
        return obj

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
            if not self._restore_tasks:
                self._to_restore.append(task)
            return
        self._to_restore = [task]
        while self._to_restore:
            task = self._to_restore.pop()
            self._post_create(task)
        self._to_restore = None

    def _get_result(self, row: TaskRow) -> Optional[object]:
        if State[row.state] < State.HAS_RUN:
            return None
        assert row.result_type
        result_type = ResultType[row.result_type]
        if result_type is ResultType.PICKLED:
            assert isinstance(row.result, bytes)
            result: object = pickle.loads(row.result)
        else:
            assert result_type is ResultType.HASHED
            assert isinstance(row.result, str)
            result = self._get_object(row.result)
        return result

    def _post_create(self, task: Task[object]) -> None:
        assert self._to_restore is not None
        row = self._get_task_row(task.hashid)
        if not row:
            assert not self._to_restore
            if self._eager:
                self._store_tasks([task])
                self._store_objects([task])
                self._db.commit()
            else:
                self._pending.add(task.hashid)
            return
        task._label = row.label
        if State[row.state] < State.HAS_RUN:
            return
        log.info(f'Restoring from cache: {task}')
        assert State[row.state] > State.HAS_RUN
        task.set_running()
        if self._restore_tasks and row.side_effects:
            with self._app.running_task_ctx(task):
                for hashid in reversed(row.side_effects.split(',')):
                    child_task = self._get_object(cast(Hash, hashid))
                    assert isinstance(child_task, Task)
                    self._to_restore.append(child_task)
        task.set_has_run()
        self._app.set_result(task, self._get_result(row))

    def post_task_run(self, task: Task[object]) -> None:
        if not self._eager:
            return
        self._store_result(task)
        self._update_state(task)
        if task.state < State.DONE:
            task.add_done_callback(lambda task: self._update_state(task))
        self._db.commit()

    def store_pending(self) -> None:
        tasks = [self._app.get_task(hashid) for hashid in self._pending]
        self._store_tasks(tasks)
        for task in tasks:
            self._update_state(task)
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
