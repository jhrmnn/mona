# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import importlib
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

from .argparse_cli import CLI, CLIError, partial
from .app import Caf, RemoteNotExists
from .Utils import get_timestamp
from .Logging import error
from .Remote import Remote
from . import cmds


def mod_remote_args(app: Caf, args: List[str], kwargs: Dict[str, Any]) -> None:
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


class NoAppFoundError(Exception):
    pass


def main() -> None:
    args = sys.argv[1:]
    app_module_path = os.environ.get('CAF_APP')
    if not app_module_path:
        if Path('app.py').is_file():
            app_module_path = 'app'
            sys.path.append('')
        else:
            raise NoAppFoundError()
    app_module = importlib.import_module(app_module_path)
    app: Caf = app_module.app  # type: ignore
    cli = CLI([
        ('conf', partial(cmds.configure, app)),
        ('run', partial(cmds.run, app)),
        ('make', partial(cmds.make, app)),
        ('dispatch', partial(cmds.dispatch, app)),
        ('checkout', partial(cmds.checkout, app)),
        ('submit', partial(cmds.submit, app)),
        ('reset', partial(cmds.reset, app)),
        ('list', [
            ('profiles', partial(cmds.list_profiles, app)),
            ('remotes', partial(cmds.list_remotes, app)),
            ('builds', partial(cmds.list_builds, app)),
            ('tasks', partial(cmds.list_tasks, app)),
        ]),
        ('status', partial(cmds.status, app)),
        ('gc', partial(cmds.gc, app)),
        ('cmd', partial(cmds.cmd, app)),
        ('remote', [
            ('add', partial(cmds.remote_add, app)),
            ('path', partial(cmds.remote_path, app)),
            ('list', partial(cmds.list_remotes, app)),
        ]),
        ('update', partial(cmds.update, app)),
        ('check', partial(cmds.check, app)),
        ('fetch', partial(cmds.fetch, app)),
        ('archive', [
            ('save', partial(cmds.archive_store, app)),
        ]),
        ('go', partial(cmds.go, app)),
    ])
    if not args:
        cli.parser.print_help()
        error()
    try:
        cli.run(argv=args)
    except CLIError as e:
        clierror = e
    else:
        log(app, args)
        return

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
        cmds.check(app, remote_spec)
    for remote in remotes:
        remote.command(rargs)
