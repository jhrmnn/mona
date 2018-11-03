# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, Optional, cast, Iterable

from .sessions import Session
from .hashing import Hash, Hashed, HashResolver, HashedBytes
from .utils import make_nonwritable, Pathable

__version__ = '0.1.0'


def Source(path: Pathable, content: Union[str, bytes] = None) -> 'HashedFile':
    if content is None:
        return HashedFile.from_path(path)
    if isinstance(content, str):
        content = content.encode()
    return HashedFile.from_bytes(path, content)


class FileManager(ABC):
    @abstractmethod
    def store_path(self, path: Path, can_destroy: bool) -> 'Hash':
        ...

    @abstractmethod
    def store_bytes(self, content: bytes) -> 'Hash':
        ...

    @abstractmethod
    def get_bytes(self, hashid: Hash) -> bytes:
        ...

    @abstractmethod
    def target_in(self, path: Path, hashid: Hash, mutable: bool) -> None:
        ...

    @classmethod
    def active(cls) -> Optional['FileManager']:
        fmngr = Session.active().storage.get('file_manager')
        assert not fmngr or isinstance(fmngr, cls)
        return fmngr


class File:
    def __init__(self, hashid: Hash, path: Path, content: bytes = None):
        self._hashid = hashid
        self._path = path
        self._content = content
        if content is None:
            fmngr = FileManager.active()
            assert fmngr
            self._fmngr = fmngr

    def __repr__(self) -> str:
        return f'<File hashid={self._hashid[:6]}, path={self._path}>'

    def __str__(self) -> str:
        return str(self._path)

    @property
    def stem(self) -> str:
        return self._path.stem

    @property
    def path(self) -> Path:
        return self._path

    def read_bytes(self) -> bytes:
        if self._content is not None:
            return self._content
        return self._fmngr.get_bytes(self._hashid)

    def read_text(self) -> str:
        return self.read_bytes().decode()

    def target_in(self, path: Path, mutable: bool = False) -> None:
        target = path / self._path
        if self._content is not None:
            target.write_bytes(self._content)
            if not mutable:
                make_nonwritable(target)
        else:
            self._fmngr.target_in(target, self._hashid, mutable)


class HashedFile(Hashed[File]):
    def __init__(self, path: Path, content: Union[HashedBytes, Hash]):
        self._path = path
        self._content_hash: Hash = getattr(content, 'hashid', content)
        Hashed.__init__(self)
        self._content = content

    @property
    def spec(self) -> bytes:
        return json.dumps([str(self._path), self._content_hash]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedFile':
        path, hashid = json.loads(spec)
        fmngr = FileManager.active()
        if fmngr:
            return cls(Path(path), hashid)
        return cls(Path(path), cast(HashedBytes, resolve(hashid)))

    @property
    def value(self) -> File:
        return File(
            self._content_hash, self._path, getattr(self._content, 'value', None)
        )

    @property
    def label(self) -> str:
        return str(self._path)

    @property
    def components(self) -> Iterable['Hashed[object]']:
        if isinstance(self._content, HashedBytes):
            return (self._content,)
        return ()

    @classmethod
    def from_path(
        cls, path: Pathable, root: Union[str, Path] = None, *, can_destroy: bool = False
    ) -> 'HashedFile':
        path = Path(path)
        assert not path.is_absolute() or root
        relpath = path.relative_to(root) if root else path
        fmngr = FileManager.active()
        if fmngr:
            return HashedFile(relpath, fmngr.store_path(path, can_destroy))
        return HashedFile(relpath, HashedBytes(path.read_bytes()))

    @classmethod
    def from_bytes(cls, path: Pathable, content: bytes) -> 'HashedFile':
        path = Path(path)
        assert not path.is_absolute()
        fmngr = FileManager.active()
        if fmngr:
            return HashedFile(path, fmngr.store_bytes(content))
        return HashedFile(path, HashedBytes(content))
