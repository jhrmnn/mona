# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path

from .hashing import Hash, Hashed, HashedBytes
from .sessions import Session
from .rules import dir_task
from .utils import make_nonwritable, Pathable
from .errors import UnknownFile
from .json import registered_classes
from .rules.dirtask import FileManager as _FileManager, \
    HashedPath as _HashedPath

from typing import Dict, Union, cast, Tuple

_dir_task = dir_task.func


class StoredHashedBytes(HashedBytes):
    def __init__(self, hashid: Hash, label: str) -> None:
        Hashed.__init__(self, hashid)
        self._label = label

    @property
    def value(self) -> bytes:
        return FileManager.active().get_bytes(self._hashid)


class HashedPath(_HashedPath):
    def __init__(self, hashid: Hash, path: Path = None) -> None:
        self._hashid = hashid
        self._path = path or FileManager.active().get_path(self._hashid)

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def path(self) -> Path:
        return self._path


registered_classes[HashedPath] = (
    lambda hp: {'hashid': hp.hashid},
    lambda dct: HashedPath(cast(Hash, dct['hashid']))
)


class FileManager(_FileManager):
    def __init__(self, root: Union[str, Pathable]) -> None:
        self._root = Path(root)
        self._cache: Dict[Hash, bytes] = {}
        self._dir_task_hooks = self._wrap_args, self._wrap_files

    def _path(self, hashid: Hash) -> Path:
        return self._root/hashid[:2]/hashid[2:]

    def __contains__(self, hashid: Hash) -> bool:
        return hashid in self._cache or self._path(hashid).is_file()

    def __call__(self, sess: Session) -> None:
        sess.storage['file_manager:self'] = self
        sess.storage['hook:dir_task'] = self._dir_task_hooks

    def get_path(self, hashid: Hash) -> Path:
        path = self._path(hashid)
        if hashid in self._cache or path.is_file():
            return path
        raise UnknownFile(hashid)

    def get_bytes(self, hashid: Hash) -> bytes:
        content = self._cache.get(hashid)
        if content:
            return content
        path = self._path(hashid)
        try:
            return self._cache.setdefault(hashid, path.read_bytes())
        except FileNotFoundError:
            pass
        raise UnknownFile(hashid)

    def store_from_path(self, path: Path) -> HashedPath:
        hashed = HashedBytes(path.read_bytes())
        hashid = hashed.hashid
        if hashid not in self:
            stored_path = self._path(hashid)
            stored_path.parent.mkdir(parents=True, exist_ok=True)
            path.rename(stored_path)
            make_nonwritable(path)
        return HashedPath(hashid, stored_path)

    def _store_bytes(self, content: bytes) -> StoredHashedBytes:
        hashed = HashedBytes(content)
        hashid = hashed.hashid
        if hashid not in self:
            self._cache[hashid] = content
            path = self._path(hashid)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            make_nonwritable(path)
        return StoredHashedBytes(hashid, hashed.label)

    def _wrap_files(self, files: Dict[str, Union[bytes, Path]]
                    ) -> Dict[str, Union[StoredHashedBytes, Path]]:
        hashed_files: Dict[str, Union[StoredHashedBytes, Path]] = {}
        for filename, target in files.items():
            if isinstance(target, bytes):
                hashed_files[filename] = self._store_bytes(target)
            else:
                hashed_files[filename] = target
        return hashed_files

    def _wrap_args(
            self, args: Tuple[bytes, Dict[str, Union[bytes, Path]]]
    ) -> Tuple[StoredHashedBytes, Dict[str, Union[StoredHashedBytes, Path]]]:
        script = self._store_bytes(args[0])
        inputs = self._wrap_files(args[1])
        return script, inputs

    @staticmethod
    def active() -> 'FileManager':
        return cast(FileManager, Session.active().storage['file_manager:self'])
