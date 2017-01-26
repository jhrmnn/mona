import subprocess as sp
import os


from caflib.Logging import info, error


class Remote:
    def __init__(self, host, path, top):
        self.host = host
        self.path = path
        self.top = top

    def update(self, delete=False):
        info(f'Updating {self.host}...')
        sp.check_call(['ssh', self.host, f'mkdir -p {self.path}'])
        exclude = []
        for file in ['.cafignore', os.path.expanduser('~/.config/caf/ignore')]:
            if os.path.exists(file):
                with open(file) as f:
                    exclude.extend(l.strip() for l in f.readlines())
        cmd = [
            'rsync', '-cirl', '--copy-unsafe-links',
            '--exclude=.*', '--exclude=build', '--exclude=*.pyc',
            '--exclude=__pycache__'
        ]
        if delete:
            cmd += '--delete'
        cmd.extend(f'--exclude={patt}' for patt in exclude)
        cmd.extend(['caf', 'cscript.py', str(self.top)])
        if os.path.exists('caflib'):
            cmd.append('caflib')
        cmd.append(f'{self.host}:{self.path}')
        sp.check_call(cmd)

    def command(self, cmd, get_output=False):
        if not get_output:
            info(f'Running `./caf {cmd}` on {self.host}...')
        caller = sp.check_output if get_output else sp.check_call
        try:
            output = caller([
                'ssh', '-t', '-o', 'LogLevel=QUIET',
                self.host,
                f'sh -c "cd {self.path} && exec python3 -u caf {cmd}"'
            ])
        except sp.CalledProcessError:
            error(f'Command `{cmd}` on {self.host} ended with error')
        return output.strip() if get_output else None

    def check(self, scheduler):
        info(f'Checking {self.host}...')
        local_hashes = {
            label: hashid for hashid, (_, label, *_)
            in scheduler.get_queue().items()
        }
        remote_hashes = dict(
            reversed(l.split()) for l in self.command(
                'list tasks --both', get_output=True
            ).decode().strip().split('\n')
        )
        is_ok = True
        for path, hashid in local_hashes.items():
            if path not in remote_hashes:
                print(f'{path} does not exist on remote')
                is_ok = False
            elif remote_hashes[path] != hashid:
                print(f'{path} has a different hash on remote')
                is_ok = False
        for path, hashid in remote_hashes.items():
            if path not in local_hashes:
                print(f'{path} does not exist on local')
                is_ok = False
        if is_ok:
            info('Local tasks are on remote')
        else:
            error('Local tasks are not on remote')

    # def fetch(self, targets, cache, root, dry=False, get_all=False, follow=False, only_mark=False):
    #     info('Fetching from {}...'.format(self.host))
    #     if not get_all:
    #         there = self.command(
    #             'list tasks {} --finished --cellar {}'.format(
    #                 ' '.join(targets) if targets else '',
    #                 '--maxdepth 1' if not follow else ''
    #             ),
    #             get_output=True
    #         ).decode().split('\r\n')
    #     roots = [p for p in root.glob('*')
    #              if not targets or p.name in targets]
    #     paths = set()
    #     for task in find_tasks(*roots, stored=True, follow=follow):
    #         cellarpath = get_stored(task)
    #         if get_all or cellarpath in there:
    #             if only_mark:
    #                 with (cache/cellarpath/'.caf/remote_seal').open('w') as f:
    #                     f.write('{0.host}:{0.path}'.format(self))
    #             else:
    #                 paths.add(cellarpath)
    #     if only_mark:
    #         return
    #     cmd = ['rsync',
    #            '-cirlP',
    #            '--delete',
    #            '--exclude=*.pyc',
    #            '--exclude=.caf/env',
    #            '--exclude=__pycache__',
    #            '--dry-run' if dry else None,
    #            '--files-from=-',
    #            '{0.host}:{0.path}/{1}'.format(self, cache),
    #            str(cache)]
    #     p = sp.Popen(filter_cmd(cmd), stdin=sp.PIPE)
    #     p.communicate('\n'.join(paths).encode())
    #
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

    def go(self):
        sp.call(['ssh', '-t', self.host, f'cd {self.path} && exec $SHELL'])


class Local:
    def __init__(self):
        self.host = 'local'

    def update(self, delete=False):
        pass

    def command(self, cmd, get_output=False):
        if not get_output:
            info(f'Running `./caf {cmd}` on {self.host}...')
        caller = sp.check_output if get_output else sp.check_call
        try:
            output = caller(f'sh -c "python3 -u caf {cmd}"', shell=True)
        except sp.CalledProcessError:
            error(f'Command `{cmd}` on {self.host} ended with error')
        return output.strip() if get_output else None

    def check(self, root):
        pass

    def fetch(self, targets, cache, root, dry=False, get_all=False, follow=False):
        pass

    def push(self, targets, cache, root, dry=False):
        pass

    def go(self):
        pass
