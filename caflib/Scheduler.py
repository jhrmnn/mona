from pathlib import Path
import sqlite3
from contextlib import contextmanager
import tempfile
import shutil

from caflib.Cellar import Cellar, State
from caflib.Logging import error, debug, no_cafdir
from caflib.Utils import get_timestamp


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
    def __init__(self, path, url=None, tmpdir=None):
        try:
            self.db = sqlite3.connect(str(Path(path)/'queue.db'))
        except sqlite3.OperationalError:
            no_cafdir()
        self.cellar = Cellar(path)
        self.url = url
        self.tmpdir = tmpdir

    def execute(self, *args):
        return self.db.execute(*args)

    def executemany(self, *args):
        return self.db.executemany(*args)

    def commit(self):
        self.db.commit()

    def submit(self, tasks):
        try:
            queue = {
                hashid: row for hashid, *row
                in self.execute('select * from queue')
            }
        except sqlite3.OperationalError:
            queue = {}
        self.execute('drop table if exists queue')
        self.execute(
            'create table queue('
            'taskhash text, state integer, label text, path text, changed text'
            ')'
        )
        self.executemany(
            'insert into queue values (?,?,?,?,?)', (
                (hashid, state, label, '', get_timestamp())
                if hashid not in queue
                else (hashid, queue[hashid][0], label, *queue[hashid][2:4])
                for hashid, state, label in tasks
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

    def tasks(self, hashes=None, limit=None, nmaxerror=5):
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
            states = self.get_states()
            was_interrupted = False
            for hashid, state in states.items():
                if hashes is not None and hashid not in hashes:
                    continue
                if state != State.CLEAN:
                    continue
                task = self.cellar.get_task(hashid)
                if any(
                        states[child] != State.DONE
                        for child in task['children'].values()
                ):
                    continue
                with self.db_lock():
                    state, label = self.execute(
                        'select state, label from queue where taskhash = ?',
                        (hashid,)
                    ).fetchone()
                    if state != State.CLEAN:
                        break
                        print(f'({label} already locked!')
                    self.execute(
                        'update queue set state = ?, changed = ? '
                        'where taskhash = ?',
                        (State.RUNNING, get_timestamp(), hashid)
                    )
                if not task['command']:
                    self.cellar.seal_task(hashid, {})
                    self.execute(
                        'update queue set state = ?, changed = ?, path = "" '
                        'where taskhash = ?',
                        (State.DONE, get_timestamp(), hashid)
                    )
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
                    self.execute(
                        'update queue set state = ?, changed = ? '
                        'where taskhash = ?',
                        (State.INTERRUPTED, get_timestamp(), hashid)
                    )
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
                    self.execute(
                        'update queue set state = ?, changed = ?, path = "" '
                        'where taskhash = ?',
                        (State.DONE, get_timestamp(), hashid)
                    )
                    print(f'{get_timestamp()}: {label} finished successfully')
                elif task.state[0] == State.ERROR:
                    print(task.state[1])
                    nerror += 1
                    self.execute(
                        'update queue set state = ?, changed = ? where taskhash = ?',
                        (State.ERROR, get_timestamp(), hashid)
                    )
                    print(f'{get_timestamp()}: {label} finished with error')
                nrun += 1
                break
            else:
                print(f'No available tasks to do, quitting')
                break
            if was_interrupted:
                print(f'{get_timestamp()}: Interrupted, quitting')
                break
        print(f'Executed {nrun} tasks')

    def get_states(self):
        try:
            return dict(self.execute('select taskhash, state from queue'))
        except sqlite3.OperationalError:
            error('There is no queue.')

    def reset_task(self, hashid):
        self.execute(
            'update queue set state = ?, changed = ?, path = "" '
            'where taskhash = ?',
            (State.CLEAN, get_timestamp(), hashid)
        )
        self.commit()

    def task_done(self, hashid):
        self.execute(
            'update queue set state = ?, changed = ?, path = "" '
            'where taskhash = ?',
            (State.DONE, get_timestamp(), hashid)
        )
        self.commit()

    def get_queue(self):
        try:
            return {
                hashid: row for hashid, *row
                in self.execute('select * from queue')
            }
        except sqlite3.OperationalError:
            error('There is no queue.')
