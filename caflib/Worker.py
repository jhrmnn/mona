import json
import subprocess
import glob
from pathlib import Path
import signal
import sys
import os
from abc import ABCMeta, abstractmethod
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from http.client import HTTPSConnection
from urllib.parse import urlencode
import socket
from contextlib import contextmanager

from caflib.Utils import cd, Configuration


class Worker(metaclass=ABCMeta):
    def __init__(self, myid, root, dry=False, limit=None, debug=False):
        self.myid = myid
        self.root = Path(root)
        self.dry = dry
        self.limit = limit
        self.debug = debug
        signal.signal(signal.SIGINT, self.sigint_handler)
        signal.signal(signal.SIGTERM, self.sigint_handler)
        self.print_info('Alive and ready.')

    def sigint_handler(self, sig, frame):
        self.print_info('Interrupted, quitting.')
        sys.exit()

    def print_info(self, msg):
        print('Worker {}: {}'.format(self.myid, msg))

    def print_debug(self, msg):
        if self.debug:
            print('Worker {}: {}'.format(self.myid, msg))

    @abstractmethod
    def get_task(self):
        pass

    @abstractmethod
    def put_back(self, path):
        pass

    @abstractmethod
    def task_done(self, path):
        pass

    def work(self):
        n_done = 0
        for path in self.locked_tasks():
            self.print_info('Started working on {}...'.format(path))
            if not self.dry:
                self.run_command(path)
            self.print_info('Finished working on {}.'.format(path))
            self.task_done(path)
            n_done += 1
            if self.limit and n_done >= self.limit:
                self.print_info('Reached limit of tasks, quitting.')
                break

    def locked_tasks(self):
        while True:
            with self.get_locked_task() as path:
                if not path:
                    return
                yield path

    def run_command(self, path):
        with cd(path):
            if Path('command').is_file():
                with open('command') as f:
                    command = f.read()
            else:
                command = ''
            if Path('.caf/env').is_file():
                command = 'source .caf/env\n' + command
            with open('run.out', 'w') as stdout, \
                    open('run.err', 'w') as stderr:
                try:
                    subprocess.check_call(command,
                                          shell=True,
                                          stdout=stdout,
                                          stderr=stderr)
                    if 'CAFWAIT' in os.environ:
                        from time import sleep
                        sleep(int(os.environ['CAFWAIT']))
                except subprocess.CalledProcessError as e:
                    print(e)
                    self.print_info(
                        'error: There was an error when working on {}'.format(path))
                    with Path('.caf/error').open('w') as f:
                        f.write(self.myid + '\n')
                else:
                    with Path('.caf/seal').open('w') as f:
                        f.write(self.myid + '\n')

    @contextmanager
    def get_locked_task(self):
        skipped = set()
        for path in self.tasks(skipped):
            self.print_debug('Trying task {}...'.format(path))
            self.cwd = path
            lockpath = path/'.lock'
            if (path/'.caf/seal').is_file():
                self.print_debug('Task {} is sealed, continue.'.format(path))
            elif (path/'.caf/error').is_file():
                self.print_debug('Task {} is in error, continue.'.format(path))
            elif not all((p/'.caf/seal').is_file() for p in get_children(path)) \
                    and not self.dry:
                self.print_debug('Task {} has unsealed children, put back and continue.'
                                 .format(path))
                self.put_back(path)
                skipped.add(path)
            else:
                try:
                    lockpath.mkdir()
                except OSError:
                    self.print_debug('Task {} is locked, continue.'.format(path))
                else:
                    break  # we have acquired lock
        else:  # there is no task left
            path = None
            lockpath = None
        try:
            yield path
        finally:
            if lockpath:
                lockpath.rmdir()

    def tasks(self, skipped):
        while True:
            path = self.get_task()
            if path is None:
                self.print_info('No more tasks in queue, quitting.')
                return
            elif path in skipped:
                self.put_back(path)
                self.print_info('All tasks have been skipped, quitting.')
                return
            else:
                yield path


def get_children(path):
    with (path/'.caf/children').open() as f:
        return [path/child for child in json.load(f)]


class LocalWorker(Worker):
    def __init__(self, myid, root, targets, dry=False, limit=None,
                 maxdepth=None, debug=False):
        super().__init__(myid, root, dry, limit, debug)
        if targets:
            targetpaths = [self.root/t for t in targets]
        else:
            targetpaths = sorted(
                (Path(p) for p in glob.glob(str(self.root) + '/*')),
                reverse=True)
        self.queue = TaskQueue()
        for targetpath in targetpaths:
            if targetpath.is_symlink():
                self.queue.append(targetpath)
            else:
                for taskpath in targetpath.glob('*'):
                    self.queue.append(taskpath)

    def get_task(self):
        return self.queue.pop()

    def put_back(self, path):
        self.queue.prepend(path)

    def task_done(self, path):
        pass


class TaskQueue:
    def __init__(self, maxdepth=None):
        self.paths = []
        self.maxdepth = maxdepth

    def append(self, path, depth=1):
        if (path/'.caf/seal').is_file():
            return
        if (path/'.caf/lock').is_file():
            self.paths.append(path)
        childpaths = get_children(path)
        if not self.maxdepth or depth < self.maxdepth:
            for childpath in childpaths:
                self.append(childpath, depth+1)

    def pop(self):
        if self.paths:
            return self.paths.pop()

    def prepend(self, path):
        self.paths.insert(0, path)


curl_pushover = '\
-F "token={token:}" -F "user={user:}" -F "title=Worker" -F "message={message:}" \
https://api.pushover.net/1/messages.json >/dev/null'


class QueueWorker(Worker):
    def __init__(self, myid, root, url, dry=False, limit=None,
                 info_start=False, debug=False):
        super().__init__(myid, root, dry, limit, debug)
        conf = Configuration(os.environ['HOME'] + '/.config/caf/conf.yaml')
        self.curl = conf.get('curl')
        self.pushover = conf.get('pushover')
        self.url = url
        self.url_done = {}
        self.url_putback = {}
        self.has_warned = False
        if info_start:
            self.call_pushover('Worker #{} on {} started'
                               .format(self.myid, socket.gethostname()))
        signal.signal(signal.SIGXCPU, self.sigxcpu_handler)

    def sigxcpu_handler(self, sig, frame):
        self.print_info('Will be soon interrupted.')
        self.put_back(self.cwd)
        self.call_pushover('Worker #{} on {} will be soon interrupted'
                           .format(self.myid, socket.gethostname()))
        self.has_warned = True

    def sigint_handler(self, sig, frame):
        self.print_info('Interrupted, quitting.')
        if not self.has_warned:
            self.sigxcpu_handler(sig, frame)
        sys.exit()

    def call_url(self, url):
        if self.curl:
            subprocess.check_call(self.curl % url, shell=True)
        else:
            with urlopen(url, timeout=30):
                pass

    def call_pushover(self, msg):
        if not self.pushover:
            return
        token = self.pushover['token']
        user = self.pushover['user']
        if self.curl:
            subprocess.check_call(
                self.curl % curl_pushover.format(token=token, user=user, message=msg),
                shell=True)
        else:
            conn = HTTPSConnection('api.pushover.net:443')
            conn.request('POST',
                         '/1/messages.json',
                         urlencode({'token': token, 'user': user, 'message': msg}),
                         {'Content-type': 'application/x-www-form-urlencoded'})
            conn.getresponse()

    def get_task(self):
        if self.curl:
            try:
                response = subprocess.check_output(
                    self.curl % self.url, shell=True).decode()
            except subprocess.CalledProcessError as e:
                if e.returncode == 22:
                    return
                else:
                    raise
        else:
            try:
                with urlopen(self.url, timeout=30) as r:
                    response = r.read().decode()
            except HTTPError:
                return
            except URLError as e:
                self.print_info('error: Cannot connect to {}: {}'
                                .format(self.url, e.reason))
                return
        task, url_done, url_putback = response.split()
        taskpath = self.root/task
        self.url_done[taskpath] = url_done
        self.url_putback[taskpath] = url_putback
        return taskpath

    def put_back(self, path):
        self.call_url(self.url_putback.pop(path))

    def task_done(self, path):
        self.call_url(self.url_done.pop(path))
