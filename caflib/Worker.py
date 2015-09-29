import json
import subprocess

from caflib.Core import cd


class Worker:
    def __init__(self, path):
        self.path = path

    def work(self, targets):
        queue = []

        def enqueue(path):
            if path not in queue:
                queue.append(path)
            children = [path/x for x in json.load((path/'.caf/children').open())]
            for child in children:
                enqueue(child)

        for target in targets:
            target = self.path/target
            if target.is_symlink():
                enqueue(target)
            else:
                for task in target.glob('*'):
                    enqueue(task)
        while queue:
            p = queue.pop()
            if not (p/'.caf/lock').is_file() or (p/'.caf/seal').is_file():
                continue
            print(p)
            with cd(p):
                with open('run.out', 'w') as stdout, \
                        open('run.err', 'w') as stderr:
                    subprocess.check_call(open('command').read(),
                                          shell=True,
                                          stdout=stdout,
                                          stderr=stderr)
            (p/'.caf/seal').touch()
