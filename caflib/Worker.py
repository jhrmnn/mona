import json
import subprocess
import glob
from pathlib import Path
import signal
import sys
import os

from caflib.Utils import cd, Configuration


curl_pushover = '\
-F "token={token:}" -F "user={user:}" -F "title=Worker" -F "message={message:}" \
https://api.pushover.net/1/messages.json >/dev/null'


def call_pushover(token, user, msg, curl=None):
    if curl:
        subprocess.check_call(
            curl % curl_pushover.format(token=token, user=user, message=msg),
            shell=True)
    else:
        import http.client
        import urllib
        conn = http.client.HTTPSConnection('api.pushover.net:443')
        conn.request('POST',
                     '/1/messages.json',
                     urllib.parse.urlencode({
                         'token': token,
                         'user': user,
                         'message': msg}),
                     {'Content-type': 'application/x-www-form-urlencoded'})
        conn.getresponse()


class Worker:
    def __init__(self, myid, path):
        self.myid = myid
        self.path = path
        self.cwd = Path(path)

    def work(self, targets, dry=False, maxdepth=None, limit=None):
        queue = []

        def enqueue(path, depth=1):
            if (path/'.caf/seal').is_file():
                return
            if (path/'.caf/lock').is_file():
                queue.append(path)
            children = [path/x for x in json.load((path/'.caf/children').open())]
            if not maxdepth or depth < maxdepth:
                for child in children:
                    enqueue(child, depth+1)

        def sigint_handler(sig, frame):
            print('Worker {} interrupted, aborting.'.format(self.myid))
            if (self.cwd/'.lock').is_dir():
                (self.cwd/'.lock').rmdir()
            sys.exit()
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        print('Worker {} alive and ready.'.format(self.myid))
        if targets:
            targets = [self.path/t for t in targets]
        else:
            targets = sorted((Path(p) for p in glob.glob('{}/*'.format(self.path))),
                             reverse=True)
        for target in targets:
            if target.is_symlink():
                enqueue(target)
            else:
                for task in target.glob('*'):
                    enqueue(task)

        n = 0
        n_skipped = 0
        while queue and n_skipped < len(queue):
            path = queue.pop()
            self.cwd = path
            lock = path/'.lock'
            try:
                lock.mkdir()
            except OSError:
                continue
            if (path/'.caf/seal').is_file():
                print('Worker {}: {} alread sealed.'.format(self.myid, path))
                (path/'.lock').rmdir()
                continue
            children = [path/x for x in json.load((path/'.caf/children').open())]
            if not all((child/'.caf/seal').is_file() for child in children) and not dry:
                print('Worker {}: {} has unsealed children.'.format(self.myid, path))
                queue.insert(0, path)
                n_skipped += 1
                (path/'.lock').rmdir()
                continue
            n_skipped = 0
            print('Worker {} started working on {}...'.format(self.myid, path))
            if not dry:
                with cd(path):
                    command = open('command').read() if Path('command').is_file() else ''
                    if Path('.caf/env').is_file():
                        command = 'source .caf/env\n' + command
                    with open('run.out', 'w') as stdout, \
                            open('run.err', 'w') as stderr:
                        try:
                            subprocess.check_call(command,
                                                  shell=True,
                                                  stdout=stdout,
                                                  stderr=stderr)
                            with (path/'.caf/seal').open('w') as f:
                                print(self.myid, file=f)
                        except subprocess.CalledProcessError as e:
                            print(e)
                            print('error: There was an error when working on {}'
                                  .format(path))
                            subprocess.call(['touch', '.caf/error'])
            (path/'.lock').rmdir()
            print('Worker {} finished working on {}.'.format(self.myid, path))
            n += 1
            if limit and n >= limit:
                print('Worker {} reached limit of tasks, aborting.'.format(self.myid))
                break
        print('Worker {} has no more tasks to do, aborting.'.format(self.myid))

    def work_from_queue(self, cellar, url, dry=False, limit=None,
                        info_start=False):
        from urllib.request import urlopen
        from urllib.error import HTTPError, URLError
        from time import sleep
        import socket

        conf = Configuration(os.environ['HOME'] + '/.config/caf/conf.yaml')
        curl = conf.get('curl')
        pushover = conf.get('pushover')

        def sigint_handler(sig, frame):
            print('Worker {} interrupted, aborting.'.format(self.myid))
            if (self.cwd/'.lock').is_dir():
                (self.cwd/'.lock').rmdir()
            if pushover:
                call_pushover(pushover['token'],
                              pushover['user'],
                              'Worker #{} on {} was interrupted'
                              .format(self.myid, socket.gethostname()),
                              curl)
            sys.exit()
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        def report_done(url):
            if curl:
                subprocess.check_call(curl % url, shell=True)
            else:
                with urlopen(url, timeout=30):
                    pass

        print('Worker {} alive and ready.'.format(self.myid))
        if info_start:
            if pushover:
                call_pushover(pushover['token'],
                              pushover['user'],
                              'Worker #{} on {} started'
                              .format(self.myid, socket.gethostname()),
                              curl)

        n = 0
        while True:
            if curl:
                try:
                    response = subprocess.check_output(curl % url, shell=True).decode()
                except subprocess.CalledProcessError as e:
                    if e.returncode == 22:
                        break
                    else:
                        raise
            else:
                try:
                    with urlopen(url, timeout=30) as r:
                        response = r.read().decode()
                except HTTPError:
                    break
                except URLError as e:
                    print('error: Cannot connect to {}: {}'.format(url, e.reason))
                    return
            task, url_done = response.split()
            self.cwd = path = cellar/task
            children = [path/x for x in json.load((path/'.caf/children').open())]
            if not all((child/'.caf/seal').is_file() for child in children) and not dry:
                print('Worker {}: {} has unsealed children, waiting...'.format(self.myid, path))
                while not all((child/'.caf/seal').is_file() for child in children):
                    sleep(1)
            print('Worker {} started working on {}...'.format(self.myid, path))
            lock = path/'.lock'
            lock.mkdir()
            if not dry:
                with cd(path):
                    command = open('command').read()
                    if Path('.caf/env').is_file():
                        command = 'source .caf/env\n' + command
                    with open('run.out', 'w') as stdout, \
                            open('run.err', 'w') as stderr:
                        try:
                            subprocess.check_call(command,
                                                  shell=True,
                                                  stdout=stdout,
                                                  stderr=stderr)
                            with (path/'.caf/seal').open('w') as f:
                                print(self.myid, file=f)
                        except subprocess.CalledProcessError as e:
                            print(e)
                            print('error: There was an error when working on {}'
                                  .format(path))
            (path/'.lock').rmdir()
            print('Worker {} finished working on {}.'.format(self.myid, path))
            report_done(url_done)
            n += 1
            if limit and n >= limit:
                print('Worker {} reached limit of tasks, aborting.'.format(self.myid))
                break
        print('Worker {} has no more tasks to do, aborting.'.format(self.myid))
