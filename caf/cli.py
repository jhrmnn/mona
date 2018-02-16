# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import importlib
import sys
from typing import Any, Dict, List, Optional

from .argparse_cli import CLI, CLIError, partial
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


def main() -> Any:
    args = sys.argv[1:]
    app_module = importlib.import_module(os.environ['CAF_APP'])
    app: Caf = app_module.app  # type: ignore
    cli = CLI([
        ('conf', app.configure),
        ('make', partial(app_cmds.make, app)),
        ('dispatch', partial(app_cmds.dispatch, app)),
        ('checkout', partial(app_cmds.checkout, app)),
        ('submit', partial(app_cmds.submit, app)),
        ('reset', partial(app_cmds.reset, app)),
        ('list', [
            ('profiles', partial(app_cmds.list_profiles, app)),
            ('remotes', partial(app_cmds.list_remotes, app)),
            ('builds', partial(app_cmds.list_builds, app)),
            ('tasks', partial(app_cmds.list_tasks, app)),
        ]),
        ('status', partial(app_cmds.status, app)),
        ('gc', partial(app_cmds.gc, app)),
        ('cmd', partial(app_cmds.cmd, app)),
        ('remote', [
            ('add', partial(app_cmds.remote_add, app)),
            ('path', partial(app_cmds.remote_path, app)),
            ('list', partial(app_cmds.list_remotes, app)),
        ]),
        ('update', partial(app_cmds.update, app)),
        ('check', partial(app_cmds.check, app)),
        ('fetch', partial(app_cmds.fetch, app)),
        ('archive', [
            ('save', partial(app_cmds.archive_store, app)),
        ]),
        ('go', partial(app_cmds.go, app)),
    ])
    if not args:
        cli.parser.print_help()
        error()
    try:
        value = cli.run(argv=args)
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
