# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from pathlib import Path

from ..hashing import Hash, Hashed, HashedBytes, HashResolver
from ..sessions import Session, SessionPlugin
from ..utils import make_nonwritable, Pathable, split
from ..errors import FilesError, InvalidInput
from ..json import registered_classes
from ..rules.dirtask import FileManager as _FileManager, HashingPath as _HashingPath

from typing import Dict, Union, cast, Tuple, Iterable, List, Optional

InputTarget = Union[str, Path, bytes]
Input = Union[str, Path, Tuple[str, InputTarget]]


class StoredHashedBytes(HashedBytes):
    def __init__(self, hashid: Hash, label: str) -> None:
        self._hashid = hashid
        self._label = label

    @property
    def spec(self) -> bytes:
        return json.dumps([self._hashid, self._label]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedBytes':
        hashid, label = json.loads(spec)
        return cls(hashid, label)

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
    def spec(self) -> bytes:
        return json.dumps([self._hashid, self._label]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedPath':
        hashid, label = json.loads(spec)
        return cls(hashid, label)

    @property
    def value(self) -> HashingPath:
        return self._path

    @property
    def label(self) -> str:
        return self._label


registered_classes[HashingPath] = (
    lambda hp: {'hashid': hp.hashid},
    lambda dct: HashingPath(cast(Hash, dct['hashid'])),
)


class FileManager(_FileManager, SessionPlugin):
    name = 'file_manager'

    def __init__(self, root: Union[str, Pathable], eager: bool = True) -> None:
        self._root = Path(root).resolve()
        self._cache: Dict[Hash, bytes] = {}
        self._path_cache: Dict[Path, HashedPath] = {}
        self._eager = eager

    def __repr__(self) -> str:
        return f'<FileManager ncache={len(self._cache)}>'

    def _path(self, hashid: Hash) -> Path:
        return self._root / hashid[:2] / hashid[2:]

    def _path_primed(self, hashid: Hash) -> Path:
        path = self._path(hashid)
        path.parent.mkdir(exist_ok=True)
        return path

    def __contains__(self, hashid: Hash) -> bool:
        return hashid in self._cache or self._path(hashid).is_file()

    def post_enter(self, sess: Session) -> None:
        sess.storage['file_manager:self'] = self
        sess.storage['hook:dir_task'] = self._wrap_args
        sess.storage['dir_task:file_manager'] = self

    def get_path(self, hashid: Hash) -> Path:
        path = self._path(hashid)
        if hashid in self._cache or path.is_file():
            return path
        raise FilesError(f'Missing in manager: {hashid}')

    def get_bytes(self, hashid: Hash) -> bytes:
        try:
            return self._cache[hashid]
        except KeyError:
            pass
        path = self._path(hashid)
        try:
            return self._cache.setdefault(hashid, path.read_bytes())
        except FileNotFoundError:
            pass
        raise FilesError(f'Missing in manager: {hashid}')

    def store_from_path(self, path: Path) -> StoredHashedBytes:
        # TODO large files could be hashed more efficiently
        hashed = HashedBytes(path.read_bytes())
        hashid = hashed.hashid
        if hashid not in self:
            stored_path = self._path_primed(hashid)
            path.rename(stored_path)
            make_nonwritable(stored_path)
        return StoredHashedBytes(hashid, hashed.label)

    def store_cache(self) -> None:
        for hashid, content in self._cache.items():
            self._store_in_file(hashid, content)

    def _store_in_file(self, hashid: Hash, content: bytes) -> None:
        stored_path = self._path_primed(hashid)
        stored_path.write_bytes(content)
        make_nonwritable(stored_path)

    def _store_bytes(self, content: bytes) -> HashedBytes:
        stored_path: Optional[Path]
        hashed = HashedBytes(content)
        hashid = hashed.hashid
        if hashid not in self:
            self._cache[hashid] = content
            if self._eager:
                self._store_in_file(hashid, content)
        return hashed

    def _store_path(self, path: Path) -> HashedPath:
        try:
            return self._path_cache[path]
        except KeyError:
            pass
        hashed = self._store_bytes(path.read_bytes())
        hashed_path = HashedPath(hashed.hashid, hashed.label)
        return self._path_cache.setdefault(path, hashed_path)

    def _wrap_target(self, target: InputTarget) -> HashedPath:
        if isinstance(target, (str, Path)):
            return self._store_path(Path(target))
        hashed = self._store_bytes(target)
        stored_bytes = StoredHashedBytes(hashed.hashid, hashed.label)
        return HashedPath(stored_bytes.hashid, stored_bytes.label)

    def _wrap_inputs(self, files: Iterable[Input]) -> Dict[str, HashedPath]:
        hashed_files: Dict[str, HashedPath] = {}
        target: InputTarget
        for item in files:
            if isinstance(item, str):
                filename, target = item, Path(item)
            elif isinstance(item, Path):
                filename, target = str(item), item
            elif (
                isinstance(item, tuple)
                and len(item) == 2
                and isinstance(item[0], str)
                and isinstance(item[1], (str, Path, bytes))
            ):
                filename, target = item
            else:
                raise InvalidInput('Unknown input type: {item!r}')
            filename = str(Path(filename))  # normalize
            if filename in hashed_files:
                raise InvalidInput('Duplicite input: {filename}')
            hashed_files[filename] = self._wrap_target(target)
        return hashed_files

    def _wrap_args(
        self,
        args: Union[
            Tuple[InputTarget, Dict[str, Union[bytes, Path]]],
            Tuple[InputTarget, List[Input]],
            Tuple[InputTarget, List[Input], Dict[str, Union[str, Path]]],
        ],
    ) -> Tuple[HashedPath, Dict[str, Union[HashedPath, Path]]]:
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
