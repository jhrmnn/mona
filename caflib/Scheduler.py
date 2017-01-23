from pathlib import Path
import sqlite3
from contextlib import contextmanager
import tempfile
import shutil

from caflib.Cellar import Cellar


class Task:
    def __init__(self, task, cellar):
        self.path = Path(tempfile.mkdtemp())
        self.inputs = cellar.checkout_task(task, self.path)
        self.command = task['command']
        self.state = (0, None)

    def error(self, exc):
        self.state = (-1, exc)

    def done(self):
        self.state = (1, None)

    def cleanup(self):
        if self.state[0] == 1:
            self.outputs = {}
            for path in self.path.glob('**/*'):
                rel_path = path.relative_to(self.path)
                if str(rel_path) not in self.inputs:
                    self.outputs[str(rel_path)] = path.read_text()
            shutil.rmtree(self.path)
        else:
            print(self.state)
            self.path.rename(self.path.name)
        return self.state[0]


class Scheduler:
    def __init__(self, path):
        self.path = Path(path)
        self.db = sqlite3.connect(str(self.path/'queue.db'))
        self.cellar = Cellar(path)

    def execute(self, *args):
        return self.db.execute(*args)

    def executemany(self, *args):
        return self.db.executemany(*args)

    def commit(self):
        self.db.commit()

    def submit(self, tasks):
        self.db.isolation_level = ''
        self.execute('drop table if exists queue')
        self.execute('create table queue(taskhash text, state integer)')
        self.executemany('insert into queue values (?,?)', tasks)
        self.commit()

    @contextmanager
    def db_lock(self):
        self.db.execute('begin immediate transaction')
        try:
            yield
        finally:
            self.execute('end transaction')

    def tasks(self):
        self.db.isolation_level = None
        while True:
            states = dict(self.execute('select * from queue'))
            for hashid, state in states.items():
                if state != 0:
                    continue
                task = self.cellar.get_task(hashid)
                if any(states[child] != 1 for child in task['children'].values()):
                    continue
                with self.db_lock():
                    state, = self.execute(
                        'select state from queue where taskhash = ?',
                        (hashid,)
                    ).fetchone()
                    if state != 0:
                        break
                    self.execute(
                        'update queue set state = 2 where taskhash = ?',
                        (hashid,)
                    )
                task = Task(task, self.cellar)
                yield task
                state = task.cleanup()
                self.execute(
                    'update queue set state = ? where taskhash = ?',
                    (state, hashid)
                )
                if state == 1:
                    self.cellar.seal_task(hashid, task.outputs)
                break
            else:
                break
