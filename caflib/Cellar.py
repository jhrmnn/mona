from pathlib import Path
import os
import stat
import json
import hashlib
import sqlite3
from datetime import datetime


def get_sha1(text):
    return hashlib.sha1(text.encode()).hexdigest()


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

    def commit(self):
        self.conn.commit()

    def store_task(self, task):
        now = datetime.today().isoformat(timespec='seconds')
        hashid = None
        self.execute('insert into tasks values (?,?,?,?)', (
            hashid, json.dumps(task), now, 0
        ))

    def fetch(self, hashid):
        return json.loads(self.execute(
            'select json from tasks where hash = ?', (hashid,)
        ).fetchone()[0])

    def store_text(self, text):
        sha1 = get_sha1(text)
        path = self.objects/sha1[:2]/sha1[2:]
        if path.is_file():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as f:
            f.write(text)
        make_nonwritable(path)
