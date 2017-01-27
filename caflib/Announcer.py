import subprocess as sp
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
# from http.client import HTTPSConnection
# from urllib.parse import urlencode
import socket


from caflib.Logging import error
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
#
#     def get_task(self):
#         if self.curl:
#             try:
#                 response = subprocess.check_output(
#                     self.curl % self.url, shell=True).decode()
#             except subprocess.CalledProcessError as e:
#                 if e.returncode == 22:
#                     return None, None
#                 else:
#                     raise
#         else:
#             try:
#                 with urlopen(self.url, timeout=30) as r:
#                     response = r.read().decode()
#             except HTTPError:
#                 return None, None
#             except URLError as e:
#                 self.info(
#                     'error: Cannot connect to {}: {}'.format(self.url, e.reason)
#                 )
#                 return None, None
#         task, label, url_state, url_putback = response.split()
#         self.url_state[task] = url_state
#         self.url_putback[task] = url_putback
#         return label, task
#
#     def put_back(self, label, taskid):
#         self.call_url(self.url_putback.pop(taskid))
#
#     def task_done(self, taskid):
#         self.call_url(self.url_state.pop(taskid) + '?state=Done')
#
#     def task_error(self, taskid):
#         self.call_url(self.url_state.pop(taskid) + '?state=Error')


class Announcer:
    def __init__(self, url, curl=None):
        self.curl = curl
        self.url, self.num = url.split(':') if ':' in url else (url, None)

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
        r = self.call_url(f'/queue/{self.num}/get?caller={socket.gethostname()}')
        hashid, *_ = r.split()
        return hashid

    def put_back(self, hashid):
        self.call_url(f'/queue/{self.num}/put_back/{hashid}')

    def task_done(self, hashid):
        self.call_url(f'/queue/{self.num}/change_state/{hashid}?state=Done')

    def task_error(self, hashid):
        self.call_url(f'/queue/{self.num}/change_state/{hashid}?state=Error')

    def submit(self, hashes):
        data = '\n'.join(reversed(
            [f'{label} {hashid}' for hashid, label in hashes.items()]
        )).encode()
        return self.call_url('/submit', data=data).strip()
