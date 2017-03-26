from pathlib import Path
import json
import sqlite3
import hashlib
from datetime import datetime
from collections import defaultdict, OrderedDict
import sys
import os
import shutil

from caflib.Logging import info, no_cafdir
from caflib.Utils import make_nonwritable
from caflib.Timing import timing
from caflib.Glob import match_glob


class State:
    CLEAN = 0
    DONE = 1
    DONEREMOTE = 5
    ERROR = -1
    RUNNING = 2
    INTERRUPTED = 3
    color = {
        CLEAN: 'normal',
        DONE: 'green',
        DONEREMOTE: 'cyan',
        ERROR: 'red',
        RUNNING: 'yellow',
        INTERRUPTED: 'blue',
    }


def get_hash(text):
    return hashlib.sha1(text.encode()).hexdigest()


def get_hash_bytes(text):
    return hashlib.sha1(text).hexdigest()


class Tree(OrderedDict):
    def __init__(self, *args, objects=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.objects = objects or {}

    def dglob(self, *patterns):
        groups = defaultdict(list)
        for patt in patterns:
            matched_any = False
            for path, hashid in self.items():
                matched = match_glob(path, patt)
                if matched:
                    groups[matched].append((hashid, path))
                    matched_any = True
            if not matched_any:
                groups[patt] = []
        return groups

    def glob(self, *patterns):
        for patt in patterns:
            for path, hashid in self.items():
                if match_glob(path, patt):
                    yield hashid, path


def symlink_to(src, dst):
    return dst.symlink_to(src)


def copy_to(src, dst):
    return shutil.copyfile(src, dst)


class Cellar:
    def __init__(self, path):
        path = Path(path).resolve()
        self.objects = path/'objects'
        self.objectdb = set()
        try:
            self.db = sqlite3.connect(str(path/'index.db'))
        except sqlite3.OperationalError:
            no_cafdir()
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

    def store(self, hashid, text=None, file=None):
        if hashid in self.objectdb:
            return
        self.objectdb.add(hashid)
        path = self.objects/hashid[:2]/hashid[2:]
        if path.is_file():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        if text is not None:
            path.write_text(text)
        elif file is not None:
            file.rename(path)
        make_nonwritable(path)
        return True

    def gc(self):
        tree = self.get_tree(objects=True)
        self.execute('create temporary table retain(hash text)')
        self.executemany('insert into retain values (?)', (
            (hashid,) for hashid in tree.values()
        ))
        for task in tree.objects.values():
            for filehash in task['inputs'].values():
                self.execute('insert into retain values (?)', (filehash,))
            if 'outputs' in task:
                for filehash in task['outputs'].values():
                    self.execute('insert into retain values (?)', (filehash,))
        retain = set(r[0] for r in self.db.execute('select hash from retain'))
        all_files = {''.join(p.parts[-2:]): p for p in self.objects.glob('*/*')}
        n_files = 0
        for filehash in set(all_files.keys()) - retain:
            all_files[filehash].unlink()
            n_files += 1
        info(f'Removed {n_files} files.')
        self.db.execute(
            'delete from targets where buildid != '
            '(select id from builds order by created desc limit 1)'
        )
        self.db.execute(
            'delete from tasks '
            'where hash not in (select distinct(hash) from retain)'
        )
        self.commit()

    def store_text(self, hashid, text):
        return self.store(hashid, text=text)

    def store_file(self, hashid, file):
        return self.store(hashid, file=file)

    def seal_task(self, hashid, outputs=None, hashed_outputs=None):
        task = self.get_task(hashid)
        if outputs:
            task['outputs'] = {}
            for name, path in outputs.items():
                try:
                    with path.open() as f:
                        filehash = get_hash(f.read())
                except UnicodeDecodeError:
                    with path.open('rb') as f:
                        filehash = get_hash_bytes(f.read())
                self.store_file(filehash, path)
                task['outputs'][name] = filehash
        elif hashed_outputs:
            task['outputs'] = hashed_outputs
        self.execute(
            'update tasks set task = ?, state = ? where hash = ?',
            (json.dumps(task), State.DONE, hashid)
        )
        self.commit()

    def reset_task(self, hashid):
        task = self.get_task(hashid)
        task['outputs'] = {}
        self.execute(
            'update tasks set task = ?, state = ? where hash = ?',
            (json.dumps(task), State.CLEAN, hashid)
        )
        self.commit()

    def store_build(self, tasks, targets, inputs, labels):
        self.execute('drop table if exists current_tasks')
        self.execute('create temporary table current_tasks(hash text)')
        self.executemany('insert into current_tasks values (?)', (
            (key,) for key in tasks.keys()
        ))
        existing = [hashid for hashid, in self.execute(
            'select tasks.hash from tasks join current_tasks '
            'on current_tasks.hash = tasks.hash'
        )]
        nnew = len(tasks)-len(existing)
        info(f'Will store {nnew} new tasks.')
        if nnew > 0 and 'TIMING' not in os.environ:
            while True:
                answer = input('Continue? ["y" to confirm, "l" to list]: ')
                if answer == 'y':
                    break
                elif answer == 'l':
                    for label in sorted(
                            labels[h] for h in set(tasks)-set(existing)
                    ):
                        print(label)
                else:
                    sys.exit()
        now = datetime.today().isoformat(timespec='seconds')
        self.executemany('insert or ignore into tasks values (?,?,?,?)', (
            (hashid, json.dumps(task), now, 0) for hashid, task in tasks.items()
            # TODO sort_keys=True
        ))
        cur = self.execute('insert into builds values (?,?)', (None, now))
        buildid = cur.lastrowid
        self.executemany('insert into targets values (?,?,?)', (
            (hashid, buildid, path) for path, hashid in targets.items()
        ))
        for hashid, text in inputs.items():
            self.store_text(hashid, text)
        self.commit()
        return self.execute(
            'select tasks.hash, state from tasks join current_tasks '
            'on tasks.hash = current_tasks.hash',
        ).fetchall()

    def get_task(self, hashid):
        with timing('get_task'):
            res = self.execute(
                'select task from tasks where hash = ?',
                (hashid,)
            ).fetchone()
            if res:
                return json.loads(res[0])

    def get_tasks(self, hashes):
        hashes = list(hashes)
        if len(hashes) < 10:
            cur = self.execute(
                'select hash, task from tasks where hash in ({})'.format(
                    ','.join(len(hashes)*['?'])
                ),
                hashes
            )
        else:
            self.execute('drop table if exists current_tasks')
            self.execute('create temporary table current_tasks(hash text)')
            self.executemany('insert into current_tasks values (?)', (
                (hashid,) for hashid in hashes
            ))
            cur = self.execute(
                'select tasks.hash, task from tasks join current_tasks '
                'on current_tasks.hash = tasks.hash'
            )
        return {hashid: json.loads(task) for hashid, task in cur}

    def get_file(self, hashid):
        path = self.objects/hashid[:2]/hashid[2:]
        if hashid not in self.objectdb:
            if not path.is_file():
                raise FileNotFoundError()
        return path

    def checkout_task(self, task, path, resolve=True, nolink=False):
        copier = copy_to if nolink else symlink_to
        children = self.get_tasks(list(task['children'].values()))
        all_files = []
        for target, filehash in task['inputs'].items():
            copier(self.get_file(filehash), path/target)
            all_files.append(target)
        for target, source in task['symlinks'].items():
            copier(source, path/target)
            all_files.append(target)
        for target, (child, source) in task['childlinks'].items():
            if resolve:
                childtask = children[task['children'][child]]
                childfile = childtask['outputs'].get(
                    source, childtask['inputs'].get(source)
                )
                copier(self.get_file(childfile), path/target)
            else:
                symlink_to(Path(child)/source, path/target)
            all_files.append(target)
        if 'outputs' in task:
            for target, filehash in task['outputs'].items():
                copier(self.get_file(filehash), path/target)
                all_files.append(target)
        return all_files

    def get_build(self, nth=0):
        targets = [(hashid, Path(path)) for hashid, path in self.db.execute(
            'select taskhash, path from targets join '
            '(select id from builds order by created desc limit 1 offset ?) b '
            'on targets.buildid = b.id',
            (nth,)
        )]
        tasks = {hashid: json.loads(task) for hashid, task in self.db.execute(
            'select tasks.hash, task from tasks join '
            '(select distinct(taskhash) as hash from targets join '
            '(select id from builds order by created desc limit 1) b '
            'on targets.buildid = b.id) build '
            'on tasks.hash = build.hash'
        )}
        return tasks, targets

    def get_builds(self):
        return [created for created, in self.db.execute(
            'select created from builds order by created desc',
        )]

    def get_tree(self, objects=False, hashes=None):
        tasks, targets = self.get_build()
        if hashes:
            tasks.update(self.get_tasks(hashes))
        tree = [(str(path), hashid) for hashid, path in targets]
        while targets:
            hashid, path = targets.pop()
            for name, childhash in tasks[hashid]['children'].items():
                childpath = path/name
                tree.append((str(childpath), childhash))
                if childhash not in tasks:
                    tasks[childhash] = self.get_task(childhash)
                targets.append((childhash, childpath))
        return Tree(sorted(tree), objects=tasks if objects else None)

    def checkout(self, root, patterns=None, nth=0, finished=False, nolink=False):
        tasks, targets = self.get_build(nth=nth)
        root = Path(root).resolve()
        paths = {}
        nsymlinks = 0
        ntasks = 0
        while targets:
            hashid, path = targets.pop()
            if hashid not in tasks:
                with timing('sql'):
                    tasks[hashid] = self.get_task(hashid)
            for name, childhash in tasks[hashid]['children'].items():
                childpath = path/name
                targets.append((childhash, childpath))
            if not any(match_glob(str(path), patt) for patt in patterns):
                continue
            if finished and 'outputs' not in tasks[hashid]:
                continue
            rootpath = root/path
            if hashid in paths:
                with timing('bones'):
                    rootpath.parent.mkdir(parents=True, exist_ok=True)
                    if not rootpath.exists():
                        rootpath.symlink_to(paths[hashid])
                        nsymlinks += 1
            else:
                with timing('bones'):
                    rootpath.mkdir(parents=True)
                with timing('checkout'):
                    nsymlinks += len(self.checkout_task(
                        tasks[hashid], rootpath, resolve=False, nolink=nolink
                    ))
                    ntasks += 1
                paths[hashid] = rootpath
        info(f'Checked out {ntasks} tasks: {nsymlinks} {"files" if nolink else "symlinks"}')
