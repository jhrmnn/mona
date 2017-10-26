#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def __lib_unpack(name, header):  # type: ignore
    from base64 import b64decode
    import tarfile
    import io
    import os
    from tempfile import gettempdir
    from pathlib import Path
    import sys

    with open(__file__) as f:
        for line in f:
            if line == header:
                break
        else:
            raise RuntimeError('No packed lib')
        version = next(f).split()[2]
        files = next(f).split()[2:]
        libpath = os.environ.get('UNPACKDIR')
        if libpath:
            libpath = Path(libpath)
            files_exist = False
        else:
            tmpdir = Path(gettempdir())/f'{name}-{os.environ["USER"]}'
            if not tmpdir.is_dir():
                tmpdir.mkdir()
            libpath = tmpdir/version
            files_exist = all((libpath/path).is_file() for path in files)
        if not files_exist:
            archive = b64decode(next(f).split()[2])
            with io.BytesIO(archive) as ftar:
                tar = tarfile.open(mode='r|gz', fileobj=ftar)
                tar.extractall(str(libpath))
    sys.path.insert(0, str(libpath))


from argparse import ArgumentParser
from base64 import b64encode
from glob import iglob
from pathlib import Path
import hashlib
import io
import os
import tarfile
import sys
import inspect

from typing import Dict, Any, IO, Iterable, List

header = '# PACKEDLIB ==>\n'


def list_patterns(patterns: Iterable[str]) -> List[str]:
    return sorted(
        path
        for pattern in patterns
        for path in iglob(pattern, recursive=True)
    )


def get_version(paths: Iterable[str]) -> str:
    h = hashlib.new('md5')
    for path in paths:
        h.update(Path(path).read_bytes())
    return h.hexdigest()


def get_unpack_code(script: Path) -> str:
    name = script.name
    code = inspect.getsource(__lib_unpack)
    return f'\n\n{code}__lib_unpack({name!r}, {header!r})\n\n\n'


def pack(script: Path, version: str, paths: Iterable[str],
         out: IO[str] = sys.stdout) -> None:
    with io.BytesIO() as ftar:
        with tarfile.open(mode='w|gz', fileobj=ftar) as farchive:
            for path in paths:
                farchive.add(path)
        archive = ftar.getvalue()
    with script.open() as f:
        for line in f:
            sline = line.lstrip()
            if sline[0] == '#':
                out.write(line)
            elif sline[:3] in ["'''", '"""']:
                tag = sline[:3]
                out.write(line)
                if line.count(tag) != 2:
                    while True:
                        line = next(f)
                        out.write(line)
                        if tag in line:
                            break
            else:
                break
        out.write(get_unpack_code(script))
        out.write(line)
        out.write(f.read())
    out.write(header)
    out.write(f'# version: {version}\n')
    out.write(f'# files: {" ".join(paths)}\n')
    out.write(f'# archive: {b64encode(archive).decode()}\n')
    out.write('# <==\n')


def parse_cli() -> Dict[str, Any]:
    parser = ArgumentParser()
    arg = parser.add_argument
    arg('script', metavar='SCRIPT', type=Path)
    arg('patterns', metavar='PATTERN', nargs='*')
    arg('--dry', action='store_true')
    arg('-C', dest='startdir', metavar='dir', type=Path)
    return vars(parser.parse_args())


def main() -> None:
    args = parse_cli()
    if args['startdir']:
        os.chdir(args['startdir'])
    paths = list_patterns(args['patterns'])
    version = get_version(paths)
    if args['dry']:
        print(f'version: {version}')
        for path in paths:
            print(path)
        return
    pack(args['script'], version, paths)


if __name__ == '__main__':
    main()
