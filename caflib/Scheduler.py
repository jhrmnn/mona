# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
import sqlite3
from contextlib import contextmanager
import tempfile
import shutil

from caflib.Cellar import Cellar, State
from caflib.Logging import error, debug, no_cafdir
from caflib.Utils import get_timestamp, sample
from caflib.Announcer import Announcer

from typing import (  # noqa
    cast, Tuple, Optional, Iterable, List, Iterator, Set, Dict, Any
)
from caflib.Cellar import Hash, TPath  # noqa


class Task:
    def __init__(self, command: str, path: str) -> None:
        self.command = command
        self.path = path
        self.state: Tuple[State, Optional[str]] = (State.CLEAN, None)

    def error(self, exc: str) -> None:
        self.state = (State.ERROR, exc)

    def done(self) -> None:
        self.state = (State.DONE, None)

    def interrupt(self) -> None:
        self.state = (State.INTERRUPTED, None)


class Scheduler:
    def __init__(self, path: Path, tmpdir: str = None) -> None:
        try:
            self.db = sqlite3.connect(
                str(path/'queue.db'),
                detect_types=sqlite3.PARSE_COLNAMES
            )
        except sqlite3.OperationalError:
            no_cafdir()
        self.execute(
            'create table if not exists queue ('
            'taskhash text primary key, state integer, label text, path text, '
            'changed text, active integer'
            ') without rowid'
        )
        self.cellar = Cellar(path)
        self.tmpdir = tmpdir

    def execute(self, sql: str, *parameters: Iterable) -> sqlite3.Cursor:
        return self.db.execute(sql, *parameters)

    def executemany(self, sql: str, *seq_of_parameters: Iterable[Iterable]) -> sqlite3.Cursor:
        return self.db.executemany(sql, *seq_of_parameters)

    def commit(self) -> None:
        if self.db.isolation_level is not None:
            self.db.commit()

    def submit(self, tasks: List[Tuple[Hash, State, str]]) -> None:
        self.execute('drop table if exists current_tasks')
        self.execute('create temporary table current_tasks(hash text)')
        self.executemany('insert into current_tasks values (?)', (
            (hashid,) for hashid, *_ in tasks
        ))
        self.execute(
            'update queue set active = 0 where taskhash not in current_tasks'
        )
        self.execute(
            'delete from queue where active = 0 and state in (?, ?)',
            (State.CLEAN, State.DONE)
        )
        self.executemany(
            'insert or ignore into queue values (?,?,?,?,?,1)', (
                (hashid, state, label, '', get_timestamp())
                for hashid, state, label in tasks
            )
        )
        self.executemany(
            'update queue set active = 1, label = ? where taskhash = ?', (
                (label, hashid) for hashid, state, label in tasks
            )
        )
        self.executemany(
            'update queue set state = ? where taskhash = ?', (
                (state, hashid) for hashid, state, _ in tasks
                if state == State.DONE
            )
        )
        self.commit()

    @contextmanager
    def db_lock(self) -> Iterator[None]:
        self.db.execute('begin immediate transaction')
        try:
            yield
        finally:
            self.execute('end transaction')

    def candidate_tasks(self, states: Iterable[Hash], randomize: bool = False) \
            -> Iterator[Hash]:
        if randomize:
            yield from sample(states)
        else:
            yield from states

    def is_state_ok(self, state: State, hashid: Hash, label: str) -> bool:
        return state == State.CLEAN

    def skip_task(self, hashid: Hash) -> None:
        pass

    def tasks_for_work(
            self,
            hashes: Set[Hash] = None,
            limit: int = None,
            nmaxerror: int = 5,
            dry: bool = False,
            randomize: bool = False
    ) -> Iterator[Task]:
        self.db.commit()
        self.db.isolation_level = None
        nrun = 0
        nerror = 0
        print(f'{get_timestamp()}: Started work')
        while True:
            if nerror >= nmaxerror:
                print(f'{nerror} errors in row, quitting')
                break
            if limit and nrun >= limit:
                print(f'{nrun} tasks ran, quitting')
                break
            queue = self.get_queue()
            states = {hashid: state for hashid, (state, *_) in queue.items()}
            labels = {hashid: label for hashid, (_, label, *__) in queue.items()}
            skipped: Set[Hash] = set()
            will_continue = False
            was_interrupted = False
            debug(f'Starting candidate loop')
            for hashid in self.candidate_tasks(states, randomize=randomize):
                label = labels[hashid]
                debug(f'Got {hashid}:{label} as candidate')
                if hashid in skipped:
                    self.skip_task(hashid)
                    debug(f'{label} has been skipped before')
                    break
                else:
                    skipped.add(hashid)
                if hashes is not None and hashid not in hashes:
                    self.skip_task(hashid)
                    debug(f'{label} is in filter, skipping')
                    continue
                state = states[hashid]
                if not self.is_state_ok(state, hashid, label):
                    debug(f'{label} does not have conforming state, skipping')
                    continue
                task = self.cellar.get_task(hashid)
                assert task
                if any(
                        states[child] != State.DONE
                        for child in task.children.values()
                ):
                    self.skip_task(hashid)
                    debug(f'{label} has unsealed children, skipping')
                    continue
                if dry:
                    self.skip_task(hashid)
                    continue
                with self.db_lock():
                    state, = self.execute(
                        'select state as "[state]" from queue where taskhash = ? and active = 1',
                        (hashid,)
                    ).fetchone()
                    if state != State.CLEAN:
                        print(f'{label} already locked!')
                        will_continue = True
                        break
                    self.execute(
                        'update queue set state = ?, changed = ? '
                        'where taskhash = ?',
                        (State.RUNNING, get_timestamp(), hashid)
                    )
                if not task.command:
                    self.cellar.seal_task(hashid, {})
                    self.task_done(hashid)
                    print(f'{get_timestamp()}: {label} finished successfully')
                    continue
                tmppath = Path(tempfile.mkdtemp(
                    prefix='caftsk_', dir=self.tmpdir
                ))
                debug(f'Executing {label} in {tmppath}')
                self.execute(
                    'update queue set path = ? where taskhash = ?',
                    (str(tmppath), hashid)
                )
                inputs = self.cellar.checkout_task(task, tmppath)
                queue_task = Task(task.command, str(tmppath))
                yield queue_task
                if queue_task.state[0] == State.INTERRUPTED:
                    was_interrupted = True
                    self.task_interrupt(hashid)
                    print(f'{get_timestamp()}: {label} was interrupted')
                    break
                elif queue_task.state[0] == State.DONE:
                    outputs = {}
                    for filepath in tmppath.glob('**/*'):
                        rel_path = filepath.relative_to(tmppath)
                        if str(rel_path) not in inputs and filepath.is_file():
                            outputs[str(rel_path)] = filepath
                    self.cellar.seal_task(hashid, outputs)
                    shutil.rmtree(tmppath)
                    nerror = 0
                    self.task_done(hashid)
                    print(f'{get_timestamp()}: {label} finished successfully')
                elif queue_task.state[0] == State.ERROR:
                    print(queue_task.state[1])
                    nerror += 1
                    self.task_error(hashid)
                    print(f'{get_timestamp()}: {label} finished with error')
                skipped = set()
                nrun += 1
                will_continue = True
            if not will_continue:
                print(f'No available tasks to do, quitting')
                break
            if was_interrupted:
                print(f'{get_timestamp()}: Interrupted, quitting')
                break
        print(f'Executed {nrun} tasks')
        self.db.isolation_level = ''

    def get_states(self) -> Dict[Hash, State]:
        try:
            return dict(self.execute(
                'select taskhash, state as "[state]" from queue where active = 1'
            ))
        except sqlite3.OperationalError:
            error('There is no queue.')

    def get_queue(self) -> Dict[Hash, Tuple[State, TPath, str, str]]:
        try:
            return {
                hashid: row for hashid, *row
                in self.execute(
                    'select taskhash, state as "[state]", label, path, changed from queue '
                    'where active = 1'
                )
            }
        except sqlite3.OperationalError:
            error('There is no queue.')

    def reset_task(self, hashid: Hash) -> None:
        path, = self.execute(
            'select path from queue where taskhash = ?', (hashid,)
        ).fetchone()
        if path:
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                pass
        self.execute(
            'update queue set state = ?, changed = ?, path = "" '
            'where taskhash = ?',
            (State.CLEAN, get_timestamp(), hashid)
        )
        self.commit()

    def gc(self) -> None:
        cur = self.execute(
            'select path, taskhash from queue where state in (?,?,?)',
            (State.ERROR, State.INTERRUPTED, State.RUNNING)
        )
        for path, hashid in cur:
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                pass
        self.execute(
            'update queue set state = ?, changed = ?, path = "" '
            'where state in (?,?,?)', (
                State.CLEAN, get_timestamp(),
                State.ERROR, State.INTERRUPTED, State.RUNNING
            )
        )
        self.commit()

    def gc_all(self) -> None:
        self.execute('delete from queue where active = 0')
        self.commit()

    def task_error(self, hashid: Hash) -> None:
        self.execute(
            'update queue set state = ?, changed = ? where taskhash = ?',
            (State.ERROR, get_timestamp(), hashid)
        )
        self.commit()

    def task_done(self, hashid: Hash, remote: str = None) -> None:
        self.execute(
            'update queue set state = ?, changed = ?, path = ? '
            'where taskhash = ?', (
                State.DONE if not remote else State.DONEREMOTE,
                get_timestamp(),
                '' if not remote else f'REMOTE:{remote}',
                hashid
            )
        )
        self.commit()

    def task_interrupt(self, hashid: Hash) -> None:
        self.execute(
            'update queue set state = ?, changed = ? '
            'where taskhash = ?',
            (State.INTERRUPTED, get_timestamp(), hashid)
        )
        self.commit()


class RemoteScheduler(Scheduler):
    def __init__(self, url: str, curl: str = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.announcer = Announcer(url, curl)

    def candidate_tasks(self, states: Iterable[Hash], randomize: bool = False) \
            -> Iterator[Hash]:
        while True:
            hashid = self.announcer.get_task()
            if hashid:
                yield hashid
            else:
                return

    def is_state_ok(self, state: State, hashid: Hash, label: str) -> bool:
        if state in (State.DONE, State.DONEREMOTE):
            print(f'Task {label} already done')
            self.task_done(hashid)
            return False
        if state in (State.ERROR, State.RUNNING, State.INTERRUPTED):
            self.reset_task(hashid)
            return True
        if state == State.CLEAN:
            return True
        assert False

    def skip_task(self, hashid: Hash) -> None:
        self.announcer.put_back(hashid)

    def task_error(self, hashid: Hash) -> None:
        super().task_error(hashid)
        self.announcer.task_error(hashid)

    def task_done(self, hashid: Hash, remote: str = None) -> None:
        super().task_done(hashid)
        self.announcer.task_done(hashid)

    def task_interrupt(self, hashid: Hash) -> None:
        super().task_error(hashid)
        self.announcer.put_back(hashid)
