# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import shlex
import subprocess
from pathlib import Path
from typing import List, Optional, Union, cast

from .errors import MonaError

__all__ = ()


class Remote:
    def __init__(self, host: str, path: str) -> None:
        self._host = host
        self._path = path

    def go(self) -> None:
        subprocess.run(
            ['ssh', '-t', self._host, f'cd {self._path} && exec $SHELL'], check=True
        )

    def update(self, *, delete: bool = False, dry: bool = False) -> None:
        subprocess.run(['ssh', self._host, f'mkdir -p {self._path}'], check=True)
        excludes: List[str] = ['/.mona/', '/.git/', '/venv/']
        excludesfiles = Path('.monaignore'), Path('.gitignore')
        for file in excludesfiles:
            if file.exists():
                with file.open() as f:
                    excludes.extend(l.strip() for l in f.readlines())
        args = ['rsync', '-cirl', *(f'--exclude={excl}' for excl in excludes)]
        if delete:
            args.append('--delete')
        if dry:
            args.append('--dry-run')
        args.append('./')
        args.append(f'{self._host}:{self._path}/')
        subprocess.run(args, check=True)

    def command(
        self,
        args: List[str],
        inp: Union[str, bytes] = None,
        capture_stdout: bool = False,
    ) -> Optional[bytes]:
        cmd = ' '.join(['venv/bin/mona', *(shlex.quote(arg) for arg in args)])
        if isinstance(inp, str):
            inp = inp.encode()
        result = subprocess.run(
            ['ssh', self._host, f'cd {self._path} && exec {cmd}'],
            input=inp,
            stdout=subprocess.PIPE if capture_stdout else None,
        )
        if result.returncode:
            raise MonaError(
                f'Command `{cmd}` on {self._host} ended '
                f'with exit code {result.returncode}'
            )
        if capture_stdout:
            return cast(bytes, result.stdout)
        return None

    # def command_output(self, args: List[str], inp: str = None) -> str:
    #     output = self.command(args, inp, _get_output=True)
    #     assert output
    #     return output

    # def check(self, hashes: Dict['TPath', 'Hash']) -> None:
    #     info(f'Checking {self.host}...')
    #     remote_hashes: Dict[TPath, Hash] = {}
    #     output = self.command_output(['list', 'tasks', '**', '--no-color'])
    #     for hashid, path, *_ in (l.split() for l in output.strip().split('\n')):
    #         remote_hashes[TPath(path)] = Hash(hashid)
    #     is_ok = True
    #     for path, hashid in hashes.items():
    #         if path not in remote_hashes:
    #             print(f'{path} does not exist on remote')
    #             is_ok = False
    #         elif remote_hashes[path] != hashid:
    #             print(f'{path} has a different hash on remote')
    #             is_ok = False
    #     for path, hashid in remote_hashes.items():
    #         if path not in hashes:
    #             print(f'{path} does not exist on local')
    #             is_ok = False
    #     if is_ok:
    #         info('Local tasks are on remote')
    #     else:
    #         error('Local tasks are not on remote')
    #
    # def fetch(
    #     self, hashes: List['Hash'], files: bool = True
    # ) -> Dict['Hash', Dict[str, Any]]:
    #     info(f'Fetching from {self.host}...')
    #     tasks = {
    #         hashid: task
    #         for hashid, task in json.loads(
    #             self.command_output(['printout'], inp='\n'.join(hashes))
    #         ).items()
    #         if task.get('outputs')
    #     }
    #     if not files:
    #         info(f'Fetched {len(tasks)}/{len(hashes)} task metadata')
    #         return tasks
    #     info(f'Will fetch {len(tasks)}/{len(hashes)} tasks')
    #     if len(tasks) == 0:
    #         return {}
    #     elif input("Continue? ['y' to confirm]: ") != 'y':
    #         return {}
    #     paths = set(
    #         hashid for task in tasks.values() for hashid in task['outputs'].values()
    #     )
    #     cmd = [
    #         'rsync',
    #         '-r',
    #         '--info=progress2',
    #         '--ignore-existing',
    #         '--files-from=-',
    #         f'{self.host}:{self.path}/.caf/objects',
    #         '.caf/objects',
    #     ]
    #     sp.run(cmd, input='\n'.join(f'{p[0:2]}/{p[2:]}' for p in paths).encode())
    #     return tasks

    # def push(self, targets, cache, root, dry=False):
    #     info('Pushing to {}...'.format(self.host))
    #     roots = [p for p in root.glob('*')
    #              if not targets or p.name in targets]
    #     paths = set()
    #     for task in find_tasks(*roots, stored=True, follow=False):
    #         paths.add(get_stored(task))
    #     cmd = ['rsync',
    #            '-cirlP',
    #            '--delete',
    #            '--exclude=*.pyc',
    #            '--exclude=.caf/env',
    #            '--exclude=__pycache__',
    #            '--dry-run' if dry else None,
    #            '--files-from=-',
    #            str(cache),
    #            '{0.host}:{0.path}/{1}'.format(self, cache)]
    #     p = sp.Popen(filter_cmd(cmd), stdin=sp.PIPE)
    #     p.communicate('\n'.join(paths).encode())


# class Local(Remote):
#     def __init__(self) -> None:
#         self.host = 'local'
#
#     def update(self, top: Path, delete: bool = False, dry: bool = False) -> None:
#         pass
#
#     def command(
#         self, args: List[str], inp: str = None, _get_output: bool = False
#     ) -> Optional[str]:
#         cmd = ' '.join(arg if ' ' not in arg else repr(arg) for arg in args)
#         cmd = f"sh -c 'python3 -u caf {cmd}'"
#         if not _get_output:
#             info(f'Running `./caf {cmd}` on {self.host}...')
#         try:
#             if _get_output:
#                 output = sp.check_output(cmd, shell=True)
#             else:
#                 sp.check_call(cmd, shell=True)
#         except sp.CalledProcessError:
#             error(f'Command `{cmd}` on {self.host} ended with error')
#         return cast(str, output.strip()) if _get_output else None
#
#     def check(self, hashes: Dict['TPath', 'Hash']) -> None:
#         pass
#
#     def fetch(
#         self, hashes: List['Hash'], files: bool = True
#     ) -> Dict['Hash', Dict[str, Any]]:
#         pass
#
#     def go(self) -> None:
#         pass
