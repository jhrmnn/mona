from pathlib import Path
import json
import sqlite3
import hashlib
from datetime import datetime
from collections import defaultdict, OrderedDict


from caflib.Utils import make_nonwritable
from caflib.Timing import timing
from caflib.Glob import match_glob


def get_hash(text):
    return hashlib.sha1(text.encode()).hexdigest()


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
        hashes = list(hashes)
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

    def checkout_task(self, task, path, resolve=True):
        children = self.get_tasks(list(task['children'].values()))
        all_files = []
        for target, filehash in task['inputs'].items():
            (path/target).symlink_to(self.get_file(filehash))
            all_files.append(target)
        for target, source in task['symlinks'].items():
            (path/target).symlink_to(source)
            all_files.append(target)
        for target, (child, source) in task['childlinks'].items():
            childtask = children[task['children'][child]]
            (path/target).symlink_to(
                self.get_file(childtask['outputs'][source]) if resolve
                else Path(child)/source
            )
            all_files.append(target)
        if 'outputs' in task:
            for target, filehash in task['outputs'].items():
                (path/target).symlink_to(self.get_file(filehash))
                all_files.append(target)
        return all_files

    def get_last_build(self):
        targets = self.db.execute(
            'select taskhash, path from targets join '
            '(select id from builds order by created desc limit 1) b '
            'on targets.buildid = b.id'
        ).fetchall()
        tasks = {hashid: json.loads(task) for hashid, task in self.db.execute(
            'select tasks.hash, task from tasks join '
            '(select distinct(taskhash) as hash from targets join '
            '(select id from builds order by created desc limit 1) b '
            'on targets.buildid = b.id) build '
            'on tasks.hash = build.hash'
        )}
        return tasks, targets

    def virtual_checkout(self, objects=False, hashes=None):
        tasks, targets = self.get_last_build()
        if hashes:
            tasks.update(self.get_tasks(hashes))
        tree = [(path, hashid) for hashid, path in targets]
        while targets:
            hashid, path = targets.pop()
            for name, childhash in tasks[hashid]['children'].items():
                childpath = f'{path}/{name}'
                tree.append((childpath, childhash))
                if childhash not in tasks:
                    tasks[childhash] = self.get_task(childhash)
                targets.append((childhash, childpath))
        if objects:
            tree = [(path, tasks[hashid]) for path, hashid in tree]
        tree = OrderedDict(sorted(tree))
        return tree

    def dglob(self, *patterns, hashes=None):
        tree = self.virtual_checkout(hashes=hashes)
        all_hashids = defaultdict(list)
        for patt in patterns:
            matched_any = False
            for path, hashid in tree.items():
                matched = match_glob(path, patt)
                if matched:
                    all_hashids[matched].append(hashid)
                    matched_any = True
            if not matched_any:
                all_hashids[patt] = []
        return all_hashids

    def glob(self, *patterns, hashes=None):
        tree = self.virtual_checkout(hashes=hashes)
        for patt in patterns:
            for path, hashid in tree.items():
                if match_glob(path, patt):
                    yield hashid, path

    def checkout(self, root):
        tasks, targets = self.get_last_build()
        root = Path(root).resolve()
        paths = {}
        for hashid, path in targets:
            path = root/path
            if hashid in paths:
                with timing('bones'):
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.symlink_to(paths[hashid])
            else:
                with timing('bones'):
                    path.mkdir(parents=True)
                with timing('checkout'):
                    self.checkout_task(tasks[hashid], path, resolve=False)
                paths[hashid] = path
        queue = list(paths.items())
        while queue:
            hashid, path = queue.pop()
            for name, childhash in tasks[hashid]['children'].items():
                if childhash in paths:
                    with timing('bones'):
                        (path/name).symlink_to(paths[childhash])
                else:
                    with timing('sql'):
                        tasks[childhash] = self.get_task(childhash)
                    with timing('bones'):
                        (path/name).mkdir()
                    with timing('checkout'):
                        self.checkout_task(
                            tasks[childhash], path/name, resolve=False
                        )
                    paths[childhash] = path/name
                    queue.append((childhash, path/name))
