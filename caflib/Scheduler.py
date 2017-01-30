from pathlib import Path
import sqlite3
from contextlib import contextmanager
import tempfile
import shutil

from caflib.Cellar import Cellar, State
from caflib.Logging import error, debug, no_cafdir
from caflib.Utils import get_timestamp
from caflib.Announcer import Announcer


class Task:
    def __init__(self, command, path):
        self.command = command
        self.path = path
        self.state = (State.CLEAN, None)

    def error(self, exc):
        self.state = (State.ERROR, exc)

    def done(self):
        self.state = (State.DONE, None)

    def interrupt(self):
        self.state = (State.INTERRUPTED, None)


class Scheduler:
    def __init__(self, path, tmpdir=None):
        try:
            self.db = sqlite3.connect(str(Path(path)/'queue.db'))
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

    def execute(self, *args):
        return self.db.execute(*args)

    def executemany(self, *args):
        return self.db.executemany(*args)

    def commit(self):
        self.db.commit()

    def submit(self, tasks):
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
        self.commit()

    @contextmanager
    def db_lock(self):
        self.db.execute('begin immediate transaction')
        try:
            yield
        finally:
            self.execute('end transaction')

    def candidate_tasks(self, states):
        yield from states

    def is_state_ok(self, state, hashid, label):
        return state == State.CLEAN

    def skip_task(self, hashid):
        pass

    def tasks_for_work(self, hashes=None, limit=None, nmaxerror=5, dry=False):
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
            labels = {hashid: label for hashid, (_, label, *_) in queue.items()}
            skipped = set()
            will_continue = True
            was_interrupted = False
            debug(f'Starting candidate loop')
            for hashid in self.candidate_tasks(states.keys()):
                label = labels[hashid]
                debug(f'Got {hashid}:{label} as candidate')
                if hashid in skipped:
                    self.skip_task(hashid)
                    debug(f'{label} has been skipped before')
                    will_continue = False
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
                if any(
                        states[child] != State.DONE
                        for child in task['children'].values()
                ):
                    self.skip_task(hashid)
                    debug(f'{label} has unsealed children, skipping')
                    continue
                if dry:
                    self.skip_task(hashid)
                    continue
                with self.db_lock():
                    state, = self.execute(
                        'select state from queue where taskhash = ? and active = 1',
                        (hashid,)
                    ).fetchone()
                    if state != State.CLEAN:
                        print(f'({label} already locked!')
                        break
                    self.execute(
                        'update queue set state = ?, changed = ? '
                        'where taskhash = ?',
                        (State.RUNNING, get_timestamp(), hashid)
                    )
                if not task['command']:
                    self.cellar.seal_task(hashid, {})
                    self.task_done(hashid)
                    print(f'{get_timestamp()}: {label} finished successfully')
                    break
                tmppath = Path(tempfile.mkdtemp(
                    prefix='caftsk_', dir=self.tmpdir
                ))
                debug(f'Executing {label} in {tmppath}')
                self.execute(
                    'update queue set path = ? where taskhash = ?',
                    (str(tmppath), hashid)
                )
                inputs = self.cellar.checkout_task(task, tmppath)
                task = Task(task['command'], tmppath)
                yield task
                if task.state[0] == State.INTERRUPTED:
                    was_interrupted = True
                    self.interrupt_task(hashid)
                    print(f'{get_timestamp()}: {label} was interrupted')
                    break
                elif task.state[0] == State.DONE:
                    outputs = {}
                    for filepath in tmppath.glob('**/*'):
                        rel_path = filepath.relative_to(tmppath)
                        if str(rel_path) not in inputs:
                            outputs[str(rel_path)] = filepath
                    self.cellar.seal_task(hashid, outputs)
                    shutil.rmtree(tmppath)
                    nerror = 0
                    self.task_done(hashid)
                    print(f'{get_timestamp()}: {label} finished successfully')
                elif task.state[0] == State.ERROR:
                    print(task.state[1])
                    nerror += 1
                    self.task_error(hashid)
                    print(f'{get_timestamp()}: {label} finished with error')
                nrun += 1
                break
            else:
                debug('No conforming candidate in a candidate loop')
                will_continue = False
            if not will_continue:
                print(f'No available tasks to do, quitting')
                break
            if was_interrupted:
                print(f'{get_timestamp()}: Interrupted, quitting')
                break
        print(f'Executed {nrun} tasks')

    def get_states(self):
        try:
            return dict(self.execute(
                'select taskhash, state from queue where active = 1'
            ))
        except sqlite3.OperationalError:
            error('There is no queue.')

    def get_queue(self):
        try:
            return {
                hashid: row for hashid, *row
                in self.execute(
                    'select taskhash, state, label, path, changed from queue '
                    'where active = 1'
                )
            }
        except sqlite3.OperationalError:
            error('There is no queue.')

    def reset_task(self, hashid):
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
        if self.db.isolation_level is not None:
            self.commit()

    def task_error(self, hashid):
        self.execute(
            'update queue set state = ?, changed = ? where taskhash = ?',
            (State.ERROR, get_timestamp(), hashid)
        )
        if self.db.isolation_level is not None:
            self.commit()

    def task_done(self, hashid, remote=None):
        self.execute(
            'update queue set state = ?, changed = ?, path = ? '
            'where taskhash = ?', (
                State.DONE if not remote else State.DONEREMOTE,
                get_timestamp(),
                '' if not remote else f'REMOTE:{remote}',
                hashid
            )
        )
        if self.db.isolation_level is not None:
            self.commit()

    def task_interrupt(self, hashid):
        self.execute(
            'update queue set state = ?, changed = ? '
            'where taskhash = ?',
            (State.INTERRUPTED, get_timestamp(), hashid)
        )
        if self.db.isolation_level is not None:
            self.commit()


class RemoteScheduler(Scheduler):
    def __init__(self, url, curl, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.announcer = Announcer(url, curl)

    def candidate_tasks(self, states):
        while True:
            hashid = self.announcer.get_task()
            if hashid:
                yield hashid
            else:
                return

    def is_state_ok(self, state, hashid, label):
        if state in (State.DONE, State.DONEREMOTE):
            print(f'Task {label} already done')
            self.task_done(hashid)
            return False
        if state in (State.ERROR, State.RUNNING, State.INTERRUPTED):
            self.reset_task(hashid)
            return True
        if state == State.CLEAN:
            return True

    def skip_task(self, hashid):
        self.announcer.put_back(hashid)

    def task_error(self, hashid):
        super().task_error(hashid)
        self.announcer.task_error(hashid)

    def task_done(self, hashid):
        super().task_done(hashid)
        self.announcer.task_done(hashid)

    def task_interrupt(self, hashid):
        super().task_error(hashid)
        self.announcer.put_back(hashid)
