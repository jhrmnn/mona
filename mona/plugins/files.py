# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
import shutil
from pathlib import Path
from typing import Dict, Union

from ..errors import FilesError
from ..files import FileManager as _FileManager
from ..hashing import Hash
from ..sessions import Session, SessionPlugin
from ..utils import Pathable, make_nonwritable, make_writable

__version__ = '0.2.0'


class FileManager(_FileManager, SessionPlugin):
    """Plugin that manages storage of abstract task files in a file system."""

    name = 'file_manager'

    def __init__(self, root: Union[str, Pathable], eager: bool = True) -> None:
        self._root = Path(root).resolve()
        self._cache: Dict[Hash, bytes] = {}
        self._path_cache: Dict[Path, Hash] = {}
        self._eager = eager

    def __repr__(self) -> str:
        return f'<FileManager ncache={len(self._cache)}>'

    def _path(self, hashid: Hash, must_exist: bool = False) -> Path:
        path = self._root / hashid[:2] / hashid[2:]
        if must_exist and not path.exists():
            raise FilesError(f'Missing in manager: {hashid}')
        return path

    def _path_primed(self, hashid: Hash) -> Path:
        path = self._path(hashid)
        path.parent.mkdir(exist_ok=True)
        return path

    def __contains__(self, hashid: Hash) -> bool:
        return hashid in self._cache or self._path(hashid).is_file()

    def post_enter(self, sess: Session) -> None:  # noqa: D102
        sess.storage['file_manager'] = self

    def _store_bytes(self, hashid: Hash, content: bytes) -> None:
        stored_path = self._path_primed(hashid)
        stored_path.write_bytes(content)
        make_nonwritable(stored_path)

    def store_bytes(self, content: bytes) -> 'Hash':  # noqa: D102
        hashid = Hash(hashlib.sha1(content).hexdigest())
        if hashid not in self:
            self._cache[hashid] = content
            if self._eager:
                self._store_bytes(hashid, content)
        return hashid

    def _store_path(self, hashid: Hash, path: Path, keep: bool) -> None:
        stored_path = self._path_primed(hashid)
        if keep:
            shutil.copy(path, stored_path)
        else:
            path.rename(stored_path)
        make_nonwritable(stored_path)

    def store_path(self, path: Path, *, keep: bool) -> 'Hash':  # noqa: D102
        hashid = self._path_cache.get(path)
        if hashid:
            return hashid
        sha1 = hashlib.sha1()
        with path.open('rb') as f:
            while True:
                data = f.read(2 ** 20)
                if not data:
                    break
                sha1.update(data)
        hashid = Hash(sha1.hexdigest())
        if hashid not in self:
            # TODO this is not good with large files
            self._cache[hashid] = path.read_bytes()
            if self._eager:
                self._store_path(hashid, path, keep)
        return self._path_cache.setdefault(path, hashid)

    def get_bytes(self, hashid: Hash) -> bytes:  # noqa: D102
        try:
            return self._cache[hashid]
        except KeyError:
            pass
        path = self._path(hashid, must_exist=True)
        return self._cache.setdefault(hashid, path.read_bytes())

    def target_in(
        self, path: Path, hashid: Hash, *, mutable: bool
    ) -> None:  # noqa: D102
        content = self._cache.get(hashid)
        if content:
            path.write_bytes(content)
            if not mutable:
                make_nonwritable(path)
            return
        stored_path = self._path(hashid, must_exist=True)
        if mutable:
            shutil.copy(stored_path, path)
            make_writable(path)
        else:
            if path.exists():
                path.unlink()
            path.symlink_to(stored_path)

    def store_cache(self) -> None:  # noqa: D102
        for hashid, content in self._cache.items():
            self._store_bytes(hashid, content)
