from pathlib import Path
import json
import subprocess

from caflib.Core import cd


class Worker:
    def __init__(self, path):
        self.path = Path(path)
        self.queue = []

    def work(self):

        def inspect(path):
            children = [path/x for x in json.load((path/'.caf/children').open())]
            for child in children:
                if child not in self.queue:
                    self.queue.append(child)
            for child in children:
                inspect(child)

        for p in self.path.glob('*/*'):
            self.queue.append(p)
            inspect(p)
        while self.queue:
            p = self.queue.pop()
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
