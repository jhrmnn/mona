from pathlib import Path
import json
import sqlite3
from datetime import datetime


class Cellar:

    def __init__(self, path):
        self.path = Path(path)
        self.conn = sqlite3.connect(str(self.path))
        self.cur = self.conn.cursor()
        self.execute(
            'create table if not exists tasks ('
            'hash text primary key, json text, created text, finished integer'
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
        self.commit()

    def commit(self):
        self.conn.commit()

    def execute(self, *args):
        self.cur.execute(*args)

    def store(self, hashid, task):
        now = datetime.today().isoformat(timespec='seconds')
        self.execute('insert into tasks values (?,?,?,?)', (
            hashid, json.dumps(task), now, 'outputs' in task
        ))

    def fetch(self, hashid):
        return json.loads(self.execute(
            'select json from tasks where hash = ?', (hashid,)
        ).fetchone()[0])
