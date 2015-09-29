import json
import subprocess
import glob
from pathlib import Path
import signal
import sys

from caflib.Core import cd


class Worker:
    def __init__(self, myid, path):
        self.myid = myid
        self.path = path

    def work(self, targets):
        def sigint_handler(sig, frame):
            print('Worker {} interrupted, aborting.'.format(self.myid))
            sys.exit()
        signal.signal(signal.SIGINT, sigint_handler)

        queue = []

        def enqueue(path):
            if (path/'.caf/seal').is_file():
                return
            if path not in queue and (path/'.caf/lock').is_file():
                queue.append(path)
            children = [path/x for x in json.load((path/'.caf/children').open())]
            for child in children:
                enqueue(child)

        print('Worker {} alive and ready.'.format(self.myid))
        if targets:
            targets = [self.path/t for t in targets]
        else:
            targets = [Path(p) for p in glob.glob('{}/*'.format(self.path))]
        for target in targets:
            if target.is_symlink():
                enqueue(target)
            else:
                for task in target.glob('*'):
                    enqueue(task)
        while queue:
            path = queue.pop()
            lock = path/'.lock'
            try:
                lock.mkdir()
            except OSError:
                continue
            print('Worker {} started working on {}...'.format(self.myid, path))
            with cd(path):
                with open('run.out', 'w') as stdout, \
                        open('run.err', 'w') as stderr:
                    subprocess.check_call(open('command').read(),
                                          shell=True,
                                          stdout=stdout,
                                          stderr=stderr)
            (path/'.lock').rmdir()
            (path/'.caf/seal').touch()
            print('Worker {} finished working on {}.'.format(self.myid, path))
        print('Worker {} has no more tasks to do, aborting.'.format(self.myid))
