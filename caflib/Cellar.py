from pathlib import Path
import os
import stat
import json
import sqlite3
import hashlib
from datetime import datetime


def get_hash(text):
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
        self.objectdb = set()
        self.db = sqlite3.connect(str(self.path/'index.db'))
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
        return self.db.execute(*args)

    def executemany(self, *args):
        return self.db.executemany(*args)

    def commit(self):
        self.db.commit()

    def get_state(self, hashid):
        res = self.execute(
            'select state from tasks where hash = ?', (hashid,)
        ).fetchone()
        if not res:
            return -1
        return res[0]

    def store_text(self, hashid, text):
        if hashid in self.objectdb:
            return
        path = self.objects/hashid[:2]/hashid[2:]
        if path.is_file():
            return
        else:
            self.objectdb.add(hashid)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        make_nonwritable(path)

    def seal_task(self, hashid, outputs):
        task = self.get_task(hashid)
        task['outputs'] = {}
        for name, text in outputs.items():
            texthash = get_hash(text)
            self.store_text(texthash, text)
            task['outputs'][name] = texthash
        self.execute(
            'update tasks set task = ?, state = 1 where hash = ?',
            (json.dumps(task), hashid)
        )
        self.commit()

    def store_build(self, tasks, targets, inputs):
        now = datetime.today().isoformat(timespec='seconds')
        self.executemany('insert or ignore into tasks values (?,?,?,?)', (
            (hashid, json.dumps(task), now, 0) for hashid, task in tasks.items()
        ))
        cur = self.execute('insert into builds values (?,?)', (None, now))
        buildid = cur.lastrowid
        self.executemany('insert into targets values (?,?,?)', (
            (hashid, buildid, path) for path, hashid in targets.items()
        ))
        for hashid, text in inputs.items():
            self.store_text(hashid, text)
        self.commit()
        self.execute('drop table if exists current_tasks')
        self.execute('create temporary table current_tasks(taskhash text)')
        self.executemany('insert into current_tasks values (?)', (
            (key,) for key in tasks.keys()
        ))
        return self.execute(
            'select hash, state from tasks join current_tasks '
            'on tasks.hash = current_tasks.taskhash',
        ).fetchall()

    def get_task(self, hashid):
        res = self.execute(
            'select task from tasks where hash = ?',
            (hashid,)
        ).fetchone()
        if res:
            return json.loads(res[0])

    def get_tasks(self, hashes):
        cur = self.execute(
            'select hash, task from tasks where hash in ({})'.format(
                ','.join(len(hashes)*['?'])
            ),
            hashes
        )
        return {hashid: json.loads(task) for hashid, task in cur}

    def get_file(self, hashid):
        path = self.objects/hashid[:2]/hashid[2:]
        if hashid not in self.objectdb:
            if not path.is_file():
                raise FileNotFoundError()
        return path.resolve()
