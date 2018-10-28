# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import sqlite3
import pickle
from enum import Enum
from weakref import WeakValueDictionary
from typing import (
    Any,
    Optional,
    Set,
    NamedTuple,
    Union,
    Sequence,
    Dict,
    cast,
    Type,
    Tuple,
    TypeVar,
    List,
)

from ..sessions import Session, SessionPlugin
from ..futures import State, Future
from ..tasks import Task
from ..utils import Pathable, get_timestamp, get_fullname, import_fullname
from ..hashing import Hash, Hashed

log = logging.getLogger(__name__)

_T_co = TypeVar('_T_co', covariant=True)


class TaskRow(NamedTuple):
    hashid: Hash
    state: str
    side_effects: Optional[str] = None
    result_type: Optional[str] = None
    result: Union[Hash, bytes, None] = None


class ObjectRow(NamedTuple):
    hashid: Hash
    typetag: str
    spec: bytes


class ResultType(Enum):
    HASHED = 0
    PICKLED = 1


class SessionRow(NamedTuple):
    sessionid: int
    created: str


class TargetRow(NamedTuple):
    objectid: Hash
    sessionid: int
    label: Optional[str]
    metadata: Optional[bytes]


class CachedTask(Task[_T_co]):
    def __init__(self, hashid: Hash) -> None:
        self._hashid = hashid
        self._args = ()
        Future.__init__(self, [])  # type: ignore
        self._hook = None


class Cache(SessionPlugin):
    name = 'db_cache'

    def __init__(
        self,
        db: sqlite3.Connection,
        eager: bool = True,
        full_restore: bool = False,
        readonly: bool = False,
    ) -> None:
        self._db = db
        self._pending: Set[Hash] = set()
        self._objects: Dict[Hash, Hashed[object]] = {}
        self._eager = eager
        self._full_restore = full_restore
        self._readonly = readonly
        self._object_cache: 'WeakValueDictionary[Hash, Hashed[object]]'
        self._object_cache = WeakValueDictionary()

    def __repr__(self) -> str:
        return f'<Cache npending={len(self._pending)} nobjects={len(self._objects)}>'

    @property
    def db(self) -> sqlite3.Connection:
        return self._db

    def _store_objects_targets(self, objs: Sequence[Hashed[object]]) -> None:
        obj_rows = [
            ObjectRow(obj.hashid, get_fullname(obj.__class__), obj.spec) for obj in objs
        ]
        self._db.executemany('INSERT OR IGNORE INTO objects VALUES (?,?,?)', obj_rows)
        self._store_targets(objs)

    def _store_targets(self, objs: Sequence[Hashed[object]]) -> None:
        sessionid = Session.active().storage['cache:sessionid']
        target_rows = [
            TargetRow(
                obj.hashid,
                sessionid,
                obj.label if isinstance(obj, Task) else None,
                obj.metadata(),
            )
            for obj in objs
        ]
        self._db.executemany(
            'INSERT OR IGNORE INTO targets VALUES (?,?,?,?)', target_rows
        )

    def _update_state(self, task: Task[object]) -> None:
        self._db.execute(
            'UPDATE tasks SET state = ? WHERE hashid = ?',
            (task.state.name, task.hashid),
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
            t.hashid for t in Session.active().get_side_effects(task)
        )
        self._db.execute(
            'REPLACE INTO tasks VALUES (?,?,?,?,?)',
            TaskRow(
                task.hashid, task.state.name, side_effects, result_type.name, result
            ),
        )

    def _get_task_row(self, hashid: Hash) -> Optional[TaskRow]:
        raw_row = self._db.execute(
            'SELECT * FROM tasks WHERE hashid = ?', (hashid,)
        ).fetchone()
        if not raw_row:
            return None
        return TaskRow(*raw_row)

    def _get_target_row(self, hashid: Hash) -> TargetRow:
        raw_row = self._db.execute(
            'SELECT * FROM targets WHERE objectid = ? ORDER BY sessionid DESC LIMIT 1',
            (hashid,),
        ).fetchone()
        assert raw_row
        return TargetRow(*raw_row)

    def _get_object_factory(self, hashid: Hash) -> Tuple[bytes, Type[Hashed[object]]]:
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
        if factory is Task and not self._full_restore:
            task_row = self._get_task_row(hashid)
            assert task_row
            if State[task_row.state] > State.HAS_RUN:
                obj = CachedTask(hashid)
        if not obj:
            obj = factory.from_spec(spec, self._get_object)
        assert hashid == obj.hashid
        metadata = self._get_target_row(hashid).metadata
        if metadata is not None:
            obj.set_metadata(metadata)
        if isinstance(obj, Task):
            obj, registered = Session.active().register_task(obj)
            if registered:
                if not self._full_restore:
                    self._to_restore.append(obj)
        self._object_cache[hashid] = obj
        return obj

    def _get_result(self, row: TaskRow) -> object:
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

    def _restore_task(self, task: Task[object]) -> None:
        row = self._get_task_row(task.hashid)
        assert row
        if State[row.state] < State.HAS_RUN:
            return
        log.debug(f'Restoring from cache: {task}')
        assert State[row.state] > State.HAS_RUN
        task.set_running()
        sess = Session.active()
        if self._full_restore and row.side_effects:
            side_effects: List[Task[object]] = []
            for hashid in row.side_effects.split(','):
                child_task = self._get_object(cast(Hash, hashid))
                assert isinstance(child_task, Task)
                sess.add_side_effect_of(task, child_task)
                side_effects.append(child_task)
            self._to_restore.extend(reversed(side_effects))
        task.set_has_run()
        sess.set_result(task, self._get_result(row))

    def post_enter(self, sess: Session) -> None:
        if self._readonly:
            return
        cur = self._db.execute(
            'INSERT INTO sessions VALUES (?,?)', (None, get_timestamp())
        )
        sess.storage['cache:sessionid'] = cur.lastrowid

    def save_hashed(self, objs: Sequence[Hashed[object]]) -> None:
        if self._readonly:
            return
        if self._eager:
            self._store_objects_targets(objs)
            self._db.commit()
        else:
            self._objects.update({o.hashid: o for o in objs})

    def post_create(self, task: Task[object]) -> None:
        row = self._get_task_row(task.hashid)
        if row:
            self._to_restore = [task]
            restored: List[Task[object]] = []
            while self._to_restore:
                t = self._to_restore.pop()
                self._restore_task(t)
                restored.append(t)
            delattr(self, '_to_restore')
            if self._readonly:
                return
            self._store_targets(restored)
            self._db.commit()
            return
        if self._readonly:
            return
        if self._eager:
            self._db.execute(
                'INSERT INTO tasks VALUES (?,?,?,?,?)',
                TaskRow(task.hashid, task.state.name),
            )
            self._store_objects_targets([task])
            self._db.commit()
        else:
            self._pending.add(task.hashid)

    def post_task_run(self, task: Task[object]) -> None:
        if self._readonly or not self._eager:
            return
        self._store_result(task)
        if task.state < State.DONE:
            task.add_done_callback(lambda task: self._update_state(task))
        self._db.commit()

    def pre_exit(self, sess: Session) -> None:
        if self._readonly or self._eager:
            return
        tasks = [sess.get_task(hashid) for hashid in self._pending]
        for task in tasks:
            if task.state > State.HAS_RUN:
                self._store_result(task)
            else:
                self._update_state(task)
        self._store_objects_targets([*self._objects.values(), *tasks])
        self._pending.clear()
        self._objects.clear()
        self._db.commit()

    @classmethod
    def from_path(cls, path: Pathable, **kwargs: Any) -> 'Cache':
        db = sqlite3.connect(path)
        db.execute(
            """\
            CREATE TABLE IF NOT EXISTS objects (
                hashid  TEXT PRIMARY KEY,
                typetag TEXT,
                spec    BLOB
            )
            """
        )
        db.execute(
            """\
            CREATE TABLE IF NOT EXISTS tasks (
                hashid       TEXT PRIMARY KEY,
                state        TEXT,
                side_effects TEXT,
                result_type  TEXT,
                result       BLOB,
                    FOREIGN KEY (hashid) REFERENCES objects(hashid)
            )
            """
        )
        db.execute(
            """\
            CREATE TABLE IF NOT EXISTS sessions (
                sessionid INTEGER PRIMARY KEY,
                created   TEXT
            )
            """
        )
        db.execute(
            """\
            CREATE TABLE IF NOT EXISTS targets (
                objectid  TEXT,
                sessionid INTEGER,
                label     TEXT,
                metadata  BLOB,
                    PRIMARY KEY (objectid, sessionid),
                    FOREIGN KEY (objectid) REFERENCES objects(hashid),
                    FOREIGN KEY (sessionid) REFERENCES sessions(sessionid)
            )
            """
        )
        return Cache(db, **kwargs)
