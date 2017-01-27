import subprocess as sp
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
import socket

from caflib.Logging import error


class Announcer:
    def __init__(self, url, num=None, curl=None):
        self.curl = curl
        self.url = url

    def call_url(self, url, data=None):
        url = f'{self.url}{url}'
        if self.curl:
            if data:
                error('Cannot send data with custom curl')
            try:
                return sp.run(
                    self.curl % url, shell=True, check=True
                ).stdout.decode()
            except sp.CalledProcessError as exc:
                if exc.returncode == 22:
                    return
                else:
                    raise
        else:
            try:
                with urlopen(url, timeout=30, data=data) as req:
                    return req.read().decode()
            except HTTPError:
                return
            except URLError as exc:
                print(f'error: Cannot connect to {self.url}: {exc.reason}')
                return

    def get_task(self):
        r = self.call_url(f'/get?caller={socket.gethostname()}')
        if not r:
            return
        hashid, *_ = r.split()
        return hashid

    def put_back(self, hashid):
        self.call_url(f'/put_back/{hashid}')

    def task_done(self, hashid):
        self.call_url(f'/change_state/{hashid}?state=Done')

    def task_error(self, hashid):
        self.call_url(f'/change_state/{hashid}?state=Error')

    def submit(self, hashes):
        data = '\n'.join(reversed(
            [f'{label} {hashid}' for hashid, label in hashes.items()]
        )).encode()
        return self.call_url('/submit', data=data).strip()


# from http.client import HTTPSConnection
# from urllib.parse import urlencode
# from contextlib import contextmanager
#
#
#
# curl_pushover = """\
# -F "token={token:}" -F "user={user:}" -F "title=Worker" -F "message={message:}" \
# https://api.pushover.net/1/messages.json >/dev/null"""
#
#
# class QueueWorker(Worker):
#     verify_lock = False
#
#     def __init__(self, myid, root, url, dry=False, limit=None, debug=False):
#         super().__init__(myid, root, dry, limit, debug)
#         conf = Configuration(os.environ['HOME'] + '/.config/caf/conf.yaml')
#         self.curl = conf.get('curl')
#         self.pushover = conf.get('pushover')
#         self.url = url + '?caller=' + socket.gethostname()
#         self.url_state = {}
#         self.url_putback = {}
#         self.has_warned = False
#         signal(SIGXCPU, self.signal_handler)
#
#     def interrupt(self):
#         self.call_pushover(
#             'Worker #{} on {} will be soon interrupted'
#             .format(self.myid, socket.gethostname())
#         )
#         self.put_back(None, self.current_taskid)
#         sys.exit()
#
#     def call_pushover(self, msg):
#         if not self.pushover:
#             return
#         token = self.pushover['token']
#         user = self.pushover['user']
#         if self.curl:
#             subprocess.check_call(
#                 self.curl % curl_pushover.format(token=token, user=user, message=msg),
#                 shell=True
#             )
#         else:
#             conn = HTTPSConnection('api.pushover.net:443')
#             conn.request(
#                 'POST',
#                 '/1/messages.json',
#                 urlencode({'token': token, 'user': user, 'message': msg}),
#                 {'Content-type': 'application/x-www-form-urlencoded'}
#             )
#             conn.getresponse()
