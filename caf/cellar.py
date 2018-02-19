# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
import json
import sqlite3
from collections import defaultdict
import sys
import shutil
from textwrap import dedent
from itertools import chain

from .Logging import info, no_cafdir
from .Utils import make_nonwritable, get_timestamp
from .Glob import match_glob
from .cellar_common import (
    State, get_hash, TPath, Hash, TaskObject, Configuration, TimeStamp
)
from .app import Caf
from .executors import Executor
from . import asyncio as _asyncio

from typing import (
    Dict, Tuple, List, DefaultDict, Iterable, Any, Awaitable,
    Iterator, Set, Optional, Union, Callable, TypeVar, NamedTuple, overload
)

_T = TypeVar('_T')


class UnfinishedTask(Exception):
    pass


@overload
async def collect(coros: Iterable[Awaitable[_T]]) -> List[Optional[_T]]: ...


@overload
async def collect(coros: Iterable[Awaitable[_T]], unfinished: _T) -> List[_T]: ...


async def collect(coros, unfinished=None):  # type: ignore
    results = await _asyncio.gather(*coros, returned_exception=UnfinishedTask)
    return [unfinished if isinstance(r, UnfinishedTask) else r for r in results]


class Tree(Dict[TPath, Hash]):
    def __init__(
            self,
            hashes: Iterable[Tuple[TPath, Hash]],
            objects: Dict[Hash, TaskObject] = None
    ) -> None:
        super().__init__(hashes)
        self.objects = objects or {}

    def dglob(self, *patterns: str) -> Dict[str, List[Tuple[Hash, str]]]:
        groups: DefaultDict[str, List[Tuple[Hash, str]]] = defaultdict(list)
        for patt in patterns:
            matched_any = False
            for path, hashid in self.items():
                matched = match_glob(path, patt)
                if matched:
                    groups[matched].append((hashid, path))
                    matched_any = True
            if not matched_any:
                groups[patt] = []
        return groups

    def glob(self, *patterns: str) -> Iterator[Tuple[Hash, TPath]]:
        for patt in patterns:
            for path, hashid in self.items():
                if match_glob(path, patt):
                    yield hashid, path


def symlink_to(src: Union[str, Path], dst: Path) -> None:
    dst.symlink_to(src)


def copy_to(src: Path, dst: Path) -> None:
    shutil.copyfile(src, dst)


class VirtualOutput:
    def __init__(self, task_hash: Hash, name: str) -> None:
        self._task_hash = task_hash
        self._name = name

    def read_bytes(self) -> bytes:
        raise UnfinishedTask()

    @property
    def path(self) -> Path:
        raise UnfinishedTask()

    def get_hash(self) -> Hash:
        return Hash(f'@{self._task_hash}/{self._name}')


class FakeOutputs:
    def __init__(self, task_hash: Hash) -> None:
        self._task_hash = task_hash

    def __getitem__(self, name: str) -> VirtualOutput:
        return VirtualOutput(self._task_hash, name)


class StoredOutput(VirtualOutput):
    def __init__(self, task_hash: Hash, name: str, cellar: 'Cellar', hashid: Hash) -> None:
        super().__init__(task_hash, name)
        self._cellar = cellar
        self._hash = hashid

    def read_bytes(self) -> bytes:
        return self.path.read_bytes()

    @property
    def path(self) -> Path:
        return self._cellar.get_file(self._hash)


class Cache(NamedTuple):
    tasks: Dict[Hash, Tuple[str, bytes]] = {}
    files: Dict[Path, Hash] = {}
    contents: Dict[Hash, bytes] = {}


class Cellar:
    unfinished_exc = UnfinishedTask

    def __init__(self, app: Caf, hook: bool = False) -> None:
        path = app.cafdir.resolve()
        self.objects = path/'objects'
        self.objectdb: Set[Hash] = set()
        try:
            self.db = sqlite3.connect(
                str(path/'index.db'),
                detect_types=sqlite3.PARSE_COLNAMES,
                timeout=30.0,
            )
        except sqlite3.OperationalError:
            no_cafdir()
        self.execute(dedent(
            """\
            CREATE TABLE IF NOT EXISTS tasks (
                hash    TEXT,
                execid  TEXT,
                state   INTEGER,
                created TEXT,
                inp     BLOB,
                out     BLOB,
                PRIMARY KEY (hash)
            )
            """
        ))
        self.execute(
            'create table if not exists builds ('
            'id integer primary key, created text'
            ')'
        )
        self.execute(
            'create table if not exists targets ('
            'taskhash text, buildid integer, path text, '
            'foreign key(taskhash) references tasks(hash), '
            'foreign key(buildid) references builds(id)'
            ')'
        )
        if hook:
            self._app = app
            self._cache = Cache()
            app.register_hook('cache')(self._cache_hook)
            app.register_hook('postget')(self._save_cache)

    @property
    def _cached(self) -> bool:
        return self._app.ctx.conf_only

    def _save_cache(self) -> None:
        if not self._cached:
            return
        cache = self._cache
        file_hashes = set(chain(cache.files.values(), cache.contents.keys()))
        new_files = set((
            hs for hs in file_hashes
            if not (self.objects/hs[:2]/hs[2:]).is_file()
        ))
        info(f'Will store {len(cache.tasks)} new tasks and {len(new_files)} new files.')
        if not cache.tasks:
            return
        if input('Continue? ["y" to confirm]: ') != 'y':
            sys.exit(1)
        now = get_timestamp()
        self.executemany('insert into tasks values (?,?,?,?,?,?)', (
            (hashid, execid, State.CLEAN, now, inp, None)
            for hashid, (execid, inp) in cache.tasks.items()
        ))
        self.commit()
        for path, hs in cache.files.items():
            if hs not in new_files:
                continue
            assert self.store_bytes(hs, path.read_bytes())
        for hs, contents in cache.contents.items():
            if hs not in new_files:
                continue
            assert self.store_bytes(hs, contents)

    async def _cache_hook(self, exe: Executor, inp: bytes) -> bytes:
        now = get_timestamp()
        hashid = get_hash(inp)
        if not self._cached:
            self.execute(
                'insert or ignore into tasks values (?,?,?,?,?,?)',
                (hashid, exe.name, State.CLEAN, now, inp, None)
            )
            self.commit()
        row: Optional[Tuple[bytes]] = self.execute(
            'select out from tasks where hash = ?', (hashid,)
        ).fetchone()
        if row and row[0] is not None:
            return row[0]
        if not row and self._cached:
            self._cache.tasks[hashid] = (exe.name, inp)
        if self._cached:
            raise UnfinishedTask()
        out = await exe(inp)
        self.execute(
            'update tasks set out = ?, state = ? where hash = ?',
            (out, State.DONE, hashid)
        )
        self.commit()
        return out

    def execute(self, sql: str, *parameters: Iterable[Any]) -> sqlite3.Cursor:
        return self.db.execute(sql, *parameters)

    def executemany(self, sql: str, *seq_of_parameters: Iterable[Iterable[Any]]) -> sqlite3.Cursor:
        return self.db.executemany(sql, *seq_of_parameters)

    def commit(self) -> None:
        self.db.commit()

    def get_state(self, hashid: Hash) -> State:
        res = self.execute(
            'select state as "[state]" from tasks where hash = ?', (hashid,)
        ).fetchone()
        if not res:
            return State.ERROR
        return State(res[0])

    def store(self, hashid: Hash, text: str = None, file: Path = None, data: bytes = None) \
            -> bool:
        if hashid in self.objectdb:
            return False
        self.objectdb.add(hashid)
        path = self.objects/hashid[:2]/hashid[2:]
        if path.is_file():
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        if text is not None:
            path.write_text(text)
        elif data is not None:
            path.write_bytes(data)
        elif file is not None:
            file.rename(path)
        make_nonwritable(path)
        return True

    def gc(self) -> None:
        tree = self.get_tree(objects=True)
        self.execute('create temporary table retain(hash text)')
        self.executemany('insert into retain values (?)', (
            (hashid,) for hashid in tree.values()
        ))
        for task in tree.objects.values():
            for filehash in task.inputs.values():
                self.execute('insert into retain values (?)', (filehash,))
            for filehash in (task.outputs or {}).values():
                self.execute('insert into retain values (?)', (filehash,))
        retain: Set[Hash] = set(
            hashid for hashid, in self.db.execute('select hash from retain')
        )
        all_files = {Hash(''.join(p.parts[-2:])): p for p in self.objects.glob('*/*')}
        n_files = 0
        for filehash in set(all_files.keys()) - retain:
            all_files[filehash].unlink()
            n_files += 1
        info(f'Removed {n_files} files.')
        self.db.execute(
            'delete from targets where buildid != '
            '(select id from builds order by created desc limit 1)'
        )
        self.db.execute(
            'delete from tasks '
            'where hash not in (select distinct(hash) from retain)'
        )
        self.commit()

    def store_text(self, hashid: Hash, text: str) -> bool:
        return self.store(hashid, text=text)

    def store_bytes(self, hashid: Hash, data: bytes) -> bool:
        return self.store(hashid, data=data)

    def store_file(self, hashid: Hash, file: Path) -> bool:
        return self.store(hashid, file=file)

    def move_file(self, file: Path) -> Hash:
        hash_ = get_hash(file.read_bytes())
        self.store_file(hash_, file)
        return hash_

    def save_file(self, file: Path) -> Hash:
        if self._cached:
            if file in self._cache.files:
                return self._cache.files[file]
            return self._cache.files.setdefault(file, get_hash(file.read_bytes()))
        return self.save_bytes(file.read_bytes())

    def save_bytes(self, contents: bytes) -> Hash:
        hash_ = get_hash(contents)
        if self._cached:
            self._cache.contents.setdefault(hash_, contents)
        else:
            self.store_bytes(hash_, contents)
        return hash_

    def get_task(self, hashid: Hash) -> Optional[TaskObject]:
        row: Optional[Tuple[str, bytes, bytes]] = self.execute(
            'select execid, inp, out from tasks where hash = ?', (hashid,)
        ).fetchone()
        if not row:
            return None
        return TaskObject.from_data(row[0], row[1], row[2])

    def _update_outputs(
            self,
            hashid: Hash,
            state: State,
            outputs: Dict[str, Hash]
    ) -> None:
        self.execute(
            'update tasks set out = ?, state = ? where hash = ?',
            (json.dumps(outputs, sort_keys=True).encode(), state, hashid)
        )
        self.commit()

    def seal_task(
            self,
            hashid: Hash,
            outputs: Dict[str, Path] = None,
            hashed_outputs: Dict[str, Hash] = None
    ) -> None:
        if outputs is not None:
            hashed_outputs = {}
            for name, path in outputs.items():
                try:
                    with path.open() as f:
                        filehash = get_hash(f.read())
                except UnicodeDecodeError:
                    with path.open('rb') as f:
                        filehash = get_hash(f.read())
                self.store_file(filehash, path)
                hashed_outputs[name] = filehash
        assert hashed_outputs is not None
        self._update_outputs(hashid, State.DONE, hashed_outputs)

    def reset_task(self, hashid: Hash) -> None:
        self._update_outputs(hashid, State.CLEAN, {})

    def store_build(self, conf: Configuration) -> Dict[Hash, State]:
        self.execute('drop table if exists current_tasks')
        self.execute('create temporary table current_tasks(hash text)')
        self.executemany('insert into current_tasks values (?)', (
            (key,) for key in conf.tasks.keys()
        ))
        existing: List[Hash] = [hashid for hashid, in self.execute(
            'select tasks.hash from tasks join current_tasks '
            'on current_tasks.hash = tasks.hash'
        )]
        nnew = len(conf.tasks)-len(existing)
        info(f'Will store {nnew} new tasks.')
        if nnew > 0:
            while True:
                answer = input('Continue? ["y" to confirm, "l" to list]: ')
                if answer == 'y':
                    break
                elif answer == 'l':
                    for label in sorted(
                            conf.labels[h] for h in set(conf.tasks)-set(existing)
                    ):
                        print(label)
                else:
                    sys.exit()
        now = get_timestamp()
        self.executemany('insert or ignore into tasks values (?,?,?,?,?,?)', (
            (hashid, task.execid, 0, now, task.data, None) for hashid, task in conf.tasks.items()
            # TODO sort_keys=True
        ))
        cur = self.execute('insert into builds values (?,?)', (None, now))
        buildid: int = cur.lastrowid
        self.executemany('insert into targets values (?,?,?)', (
            (hashid, buildid, path) for path, hashid in conf.targets.items()
        ))
        for hashid, text in conf.inputs.items():
            if isinstance(text, str):
                self.store_text(hashid, text)
            else:
                self.store_bytes(hashid, text)
        self.commit()
        return dict(self.execute(
            'select tasks.hash, state as "[state]" from tasks join current_tasks '
            'on tasks.hash = current_tasks.hash',
        ).fetchall())

    def get_tasks(self, hashes: Iterable[Hash]) -> Dict[Hash, TaskObject]:
        hashes = list(hashes)
        if len(hashes) < 10:
            cur = self.execute(
                'select execid, hash, inp, out from tasks where hash in ({})'.format(
                    ','.join(len(hashes)*['?'])
                ),
                hashes
            )
        else:
            self.execute('drop table if exists current_tasks')
            self.execute('create temporary table current_tasks(hash text)')
            self.executemany('insert into current_tasks values (?)', (
                (hashid,) for hashid in hashes
            ))
            cur = self.execute(
                'select execid, tasks.hash, inp, out from tasks join current_tasks '
                'on current_tasks.hash = tasks.hash'
            )
        return {
            hashid: TaskObject.from_data(execid, inp, out)
            for execid, hashid, inp, out in cur
        }

    def get_file(self, hashid: Hash) -> Path:
        if hashid[0] == '@':
            task_hash, name = hashid[1:].split('/', 1)
            out, = self.execute(
                'select out from tasks where hash = ?', (task_hash,)
            ).fetchone()
            assert out
            hashid = json.loads(out)[name]
        path = self.objects/hashid[:2]/hashid[2:]
        if hashid not in self.objectdb:
            if not path.is_file():
                raise FileNotFoundError(path)
        return path

    def wrap_files(self, inp: bytes, files: Dict[str, Hash]) -> Dict[str, StoredOutput]:
        task_hash = get_hash(inp)
        return {
            fname: StoredOutput(task_hash, fname, self, hs)
            for fname, hs in files.items()
        }

    def unfinished_output(self, inp: bytes) -> FakeOutputs:
        return FakeOutputs(get_hash(inp))

    def checkout_task(
            self,
            task: TaskObject,
            path: Path,
            nolink: bool = False
    ) -> List[str]:
        copier: Callable[[Path, Path], None] = copy_to if nolink else symlink_to
        children = self.get_tasks(task.children)
        all_files = []
        for target, filehash in task.inputs.items():
            fulltarget = path/target
            fulltarget.parent.mkdir(parents=True, exist_ok=True)
            copier(self.get_file(filehash), fulltarget)
            all_files.append(target)
        for target, source in task.symlinks.items():
            fulltarget = path/target
            fulltarget.parent.mkdir(parents=True, exist_ok=True)
            fulltarget.symlink_to(source)
            all_files.append(target)
        for target, (hs, source) in task.childlinks.items():
            childtask = children[hs]
            if childtask.outputs:
                childfile = childtask.outputs.get(
                    source, childtask.inputs.get(source)
                )
                assert childfile
                copier(self.get_file(childfile), path/target)
                all_files.append(target)
            else:
                symlink_to(Path(hs)/source, path/target)
        for target, filehash in (task.outputs or {}).items():
            copier(self.get_file(filehash), path/target)
            all_files.append(target)
        return all_files

    def get_build(self, nth: int = 0) \
            -> Tuple[Dict[Hash, TaskObject], List[Tuple[Hash, Path]]]:
        targets = [(hashid, Path(path)) for hashid, path in self.db.execute(
            'select taskhash, path from targets join '
            '(select id from builds order by created desc limit 1 offset ?) b '
            'on targets.buildid = b.id',
            (nth,)
        )]
        tasks = {
            hashid: TaskObject.from_data(execid, inp, out)
            for execid, hashid, inp, out in self.db.execute(
                'select execid, tasks.hash, inp, out from tasks join '
                '(select distinct(taskhash) as hash from targets join '
                '(select id from builds order by created desc limit 1) b '
                'on targets.buildid = b.id) build '
                'on tasks.hash = build.hash'
            )
        }
        return tasks, targets

    def get_builds(self) -> List[TimeStamp]:
        return [created for created, in self.db.execute(
            'select created from builds order by created desc',
        )]

    def get_tree(self, objects: bool = False, hashes: Iterable[Hash] = None) -> Tree:
        tasks, targets = self.get_build()
        if hashes:
            tasks.update(self.get_tasks(hashes))
        tree = [(TPath(str(path)), hashid) for hashid, path in targets]
        return Tree(sorted(tree), objects=tasks if objects else None)

    def checkout(
            self,
            root: Path,
            patterns: Iterable[str],
            nth: int = 0,
            finished: bool = False,
            nolink: bool = False
    ) -> None:
        tasks, targets = self.get_build(nth=nth)
        root = root.resolve()
        nsymlinks = 0
        ntasks = 0
        for hashid, path in targets:
            if not any(match_glob(str(path), patt) for patt in patterns):
                continue
            if finished and tasks[hashid].outputs is None:
                continue
            rootpath = root/path
            rootpath.mkdir(parents=True, exist_ok=True)
            nsymlinks += len(self.checkout_task(
                tasks[hashid], rootpath, nolink=nolink
            ))
            ntasks += 1
        info(f'Checked out {ntasks} tasks: {nsymlinks} {"files" if nolink else "symlinks"}')
