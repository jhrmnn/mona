# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path

from ..hashing import Hash, Hashed, HashedBytes
from ..sessions import Session, SessionPlugin
from ..rules import dir_task
from ..utils import make_nonwritable, Pathable, split
from ..errors import FilesError, InvalidInput
from ..json import registered_classes
from ..rules.dirtask import FileManager as _FileManager, \
    HashingPath as _HashingPath

from typing import Dict, Union, cast, Tuple, Iterable, List

InputTarget = Union[str, Path, bytes]
Input = Union[str, Path, Tuple[str, InputTarget]]

_dir_task = dir_task.func


class StoredHashedBytes(HashedBytes):
    def __init__(self, hashid: Hash, label: str) -> None:
        self._hashid = hashid
        self._label = label

    @property
    def value(self) -> bytes:
        return FileManager.active().get_bytes(self._hashid)


class HashingPath(_HashingPath):
    def __init__(self, hashid: Hash, path: Path = None) -> None:
        self._hashid = hashid
        self._path = path

    def __repr__(self) -> str:
        return f'<HashingPath hashid={self._hashid[:6]}>'

    @property
    def path(self) -> Path:
        if not self._path:
            self._path = FileManager.active().get_path(self._hashid)
        return self._path


class HashedPath(Hashed[HashingPath]):
    def __init__(self, hashid: Hash, label: str, path: Path = None) -> None:
        hashing_path = HashingPath(hashid, path)
        self._hashid = hashid
        self._path = hashing_path
        self._label = label

    @property
    def spec(self) -> str:
        raise NotImplementedError()

    @property
    def value(self) -> HashingPath:
        return self._path

    @property
    def label(self) -> str:
        return self._label


registered_classes[HashingPath] = (
    lambda hp: {'hashid': hp.hashid},
    lambda dct: HashingPath(cast(Hash, dct['hashid']))
)


class FileManager(_FileManager, SessionPlugin):
    name = 'file_manager'

    def __init__(self, root: Union[str, Pathable]) -> None:
        self._root = Path(root)
        self._cache: Dict[Hash, bytes] = {}
        self._source_cache: Dict[Path, HashedPath] = {}
        self._dir_task_hooks = self._wrap_args, None

    def _path(self, hashid: Hash) -> Path:
        return self._root/hashid[:2]/hashid[2:]

    def _path_primed(self, hashid: Hash) -> Path:
        path = self._path(hashid)
        path.parent.mkdir(exist_ok=True)
        return path

    def __contains__(self, hashid: Hash) -> bool:
        return hashid in self._cache or self._path(hashid).is_file()

    def post_enter(self, sess: Session) -> None:
        sess.storage['file_manager:self'] = self
        sess.storage['hook:dir_task'] = self._dir_task_hooks
        sess.storage['dir_task:file_manager'] = self

    def get_path(self, hashid: Hash) -> Path:
        path = self._path(hashid)
        if hashid in self._cache or path.is_file():
            return path
        raise FilesError(f'Missing in manager: {hashid}')

    def get_bytes(self, hashid: Hash) -> bytes:
        content = self._cache.get(hashid)
        if content:
            return content
        path = self._path(hashid)
        try:
            return self._cache.setdefault(hashid, path.read_bytes())
        except FileNotFoundError:
            pass
        raise FilesError(f'Missing in manager: {hashid}')

    def store_from_path(self, path: Path) -> StoredHashedBytes:
        # TODO large files could be hashed more efficiently and copied
        hashed = HashedBytes(path.read_bytes())
        hashid = hashed.hashid
        if hashid not in self:
            stored_path = self._path_primed(hashid)
            path.rename(stored_path)
            make_nonwritable(stored_path)
        return StoredHashedBytes(hashid, hashed.label)

    def _store_bytes(self, content: bytes) -> StoredHashedBytes:
        hashed = HashedBytes(content)
        hashid = hashed.hashid
        if hashid not in self:
            self._cache[hashid] = content
            stored_path = self._path_primed(hashid)
            stored_path.write_bytes(content)
            make_nonwritable(stored_path)
        return StoredHashedBytes(hashid, hashed.label)

    def _store_source(self, path: Path) -> HashedPath:
        hashed_path = self._source_cache.get(path)
        if hashed_path:
            return hashed_path
        content = path.read_bytes()
        hashed = HashedBytes(content)
        hashid = hashed.hashid
        if hashid not in self:
            self._cache[hashid] = content
            stored_path = self._path_primed(hashid)
            stored_path.write_bytes(content)
            make_nonwritable(stored_path)
        hashed_path = HashedPath(hashid, hashed.label, stored_path)
        return self._source_cache.setdefault(path, hashed_path)

    def _wrap_target(self, target: InputTarget) -> HashedPath:
        if isinstance(target, (str, Path)):
            return self._store_source(Path(target))
        stored_bytes = self._store_bytes(target)
        return HashedPath(stored_bytes.hashid, stored_bytes.label)

    def _wrap_inputs(self, files: Iterable[Input]) -> Dict[str, HashedPath]:
        hashed_files: Dict[str, HashedPath] = {}
        target: InputTarget
        for item in files:
            if isinstance(item, str):
                filename, target = item, Path(item)
            elif isinstance(item, Path):
                filename, target = str(item), item
            elif isinstance(item, tuple) and len(item) == 2 and \
                    isinstance(item[0], str) and \
                    isinstance(item[1], (str, Path, bytes)):
                filename, target = item
            else:
                raise InvalidInput('Unknown input type: {item!r}')
            filename = str(Path(filename))  # normalize
            if filename in hashed_files:
                raise InvalidInput('Duplicite input: {filename}')
            hashed_files[filename] = self._wrap_target(target)
        return hashed_files

    def _wrap_args(self, args: Union[
            Tuple[InputTarget, Dict[str, Union[bytes, Path]]],
            Tuple[InputTarget, List[Input]],
            Tuple[InputTarget, List[Input], Dict[str, Union[str, Path]]]
    ]) -> Tuple[HashedPath, Dict[str, Union[HashedPath, Path]]]:
        exe = args[0]
        inputs = args[1]
        stored_exe = self._wrap_target(exe)
        if isinstance(inputs, dict):
            inputs, symlinks = split(
                inputs.items(), lambda item: isinstance(item[1], bytes)
            )
        else:
            symlinks = list(args[2].items()) if len(args) == 3 else []
        stored_inputs: Dict[str, Union[HashedPath, Path]] = {}
        stored_inputs.update(self._wrap_inputs(inputs))
        for filename, target in symlinks:
            if filename in stored_inputs:
                raise InvalidInput('Duplicite input: {filename}')
            stored_inputs[filename] = Path(target)
        return stored_exe, stored_inputs

    @staticmethod
    def active() -> 'FileManager':
        return cast(FileManager, Session.active().storage['file_manager:self'])
