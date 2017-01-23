from pathlib import Path
import os
import stat
import json
import sqlite3
from datetime import datetime


def make_nonwritable(path):
    os.chmod(
        path,
        stat.S_IMODE(os.lstat(path).st_mode) &
        ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    )


class Cellar:
    def __init__(self, path):
        self.path = Path(path)
        self.objects = self.path/'objects'
        self.objectdb = set()
        self.conn = sqlite3.connect(str(self.path/'index.db'))
        self.cur = self.conn.cursor()
        self.execute(
            'create table if not exists tasks ('
            'hash text primary key, task text, created text, state integer'
            ')'
        )
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

    def execute(self, *args):
        self.cur.execute(*args)

    def executemany(self, *args):
        self.cur.executemany(*args)

    def commit(self):
        self.conn.commit()

    def get_state(self, hashid):
        res = self.execute(
            'select state from tasks where hash = ?', (hashid,)
        )
        if not res:
            return -1
        return res.fetchone()[0]

    def fetch(self, hashid):
        return json.loads(self.execute(
            'select json from tasks where hash = ?', (hashid,)
        ).fetchone()[0])

    def store_text(self, hashid, text):
        if hashid in self.objectdb:
            return
        path = self.objects/hashid[:2]/hashid[2:]
        if path.is_file():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as f:
            f.write(text)
        make_nonwritable(path)

    def store_build(self, tasks, targets, inputs):
        now = datetime.today().isoformat(timespec='seconds')
        self.executemany('insert or ignore into tasks values (?,?,?,?)', (
            (hashid, json.dumps(task), now, 0) for hashid, task in tasks.items()
        ))
        self.execute('insert into builds values (?,?)', (None, now))
        buildid = self.cur.lastrowid
        self.executemany('insert into targets values (?,?,?)', (
            (hashid, buildid, path) for path, hashid in targets.items()
        ))
        for hashid, text in inputs.items():
            self.store_text(hashid, text)
        self.commit()
