from pathlib import Path
import json
import sqlite3
from datetime import datetime


blobs = 'blobs.db'
meta = 'meta.db'


class Cellar:

    def __init__(self, path):
        self.path = Path(path)
        self.blobs = sqlite3.connect(str(self.path/blobs))
        self.cblobs = self.blobs.cursor()
        self.cblobs.execute(
            'create table if not exists blobs ('
            'hash primary key, json text'
            ')'
        )
        self.blobs.commit()
        self.meta = sqlite3.connect(str(self.path/meta))
        self.cmeta = self.meta.cursor()
        self.execute(
            'create table if not exists tasks ('
            'hash text primary key, created text, state integer'
            ') without rowid'
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

    def store(self, hashid, task):
        now = datetime.today().isoformat(timespec='seconds')
        self.execute('insert into tasks values (?,?,?,?)', (
            hashid, json.dumps(task), now, 'outputs' in task
        ))

    def fetch(self, hashid):
        return json.loads(self.execute(
            'select json from tasks where hash = ?', (hashid,)
        ).fetchone()[0])
