# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import importlib
import sys
from typing import Any, Dict, List, Optional

from .argparse_cli import CLI, CLIError
from . import app as app_cmds
from .app import Caf, RemoteNotExists
from .Utils import get_timestamp
from .Logging import error
from .Remote import Remote


def mod_remote_args(app: Caf, args: List[str], kwargs: Dict) -> None:
    if '--last' in args:
        args.remove('--last')
        args = args + ['--queue', app.last_queue]
    if args[0] == 'make' and 'url' in kwargs:
        url = kwargs['url']
        args[args.index(url)] = app.get_queue_url(url)


def log(app: Caf, args: List[str]) -> None:
    if app.cafdir.exists():
        with (app.cafdir/'log').open('a') as f:
            f.write(f'{get_timestamp()}: {" ".join(args)}\n')


cli = CLI([
    ('conf', app_cmds.conf),
    ('make', app_cmds.make),
    ('dispatch', app_cmds.dispatch),
    ('checkout', app_cmds.checkout),
    ('submit', app_cmds.submit),
    ('reset', app_cmds.reset),
    ('list', [
        ('profiles', app_cmds.list_profiles),
        ('remotes', app_cmds.list_remotes),
        ('builds', app_cmds.list_builds),
        ('tasks', app_cmds.list_tasks),
    ]),
    ('status', app_cmds.status),
    ('gc', app_cmds.gc),
    ('cmd', app_cmds.cmd),
    ('remote', [
        ('add', app_cmds.remote_add),
        ('path', app_cmds.remote_path),
        ('list', app_cmds.list_remotes),
    ]),
    ('update', app_cmds.update),
    ('check', app_cmds.check),
    ('fetch', app_cmds.fetch),
    ('archive', [
        ('save', app_cmds.archive_store),
    ]),
    ('go', app_cmds.go),
])


def main() -> Any:
    args = sys.argv[1:]
    app_module = importlib.import_module(os.environ['CAF_APP'])
    app = app_module.app  # type: ignore
    if not args:
        cli.parser.print_help()
        error()
    try:
        value = cli.run(app, argv=args)
    except CLIError as e:
        clierror = e
    else:
        log(app, args)
        return value

    remote_spec, *rargs = args
    try:
        remotes: Optional[List[Remote]] = app.parse_remotes(remote_spec)
    except RemoteNotExists:
        remotes = None
    if not rargs:
        if remotes is None:
            clierror.reraise()
        return
    try:
        kwargs = cli.parse(rargs)
    except CLIError as rclierror:
        if remotes is None:
            clierror.reraise()
        rclierror.reraise()
    if remotes is None:
        error(f'Remote {remote_spec!r} is not defined')

    mod_remote_args(app, rargs, kwargs)
    log(app, args)
    if rargs[0] in ['conf', 'make']:
        for remote in remotes:
            remote.update(app.cafdir.parent)
    if rargs[0] == 'make':
        app_cmds.check(app, remote_spec)
    for remote in remotes:
        remote.command(rargs)
