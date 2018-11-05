# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, Optional, cast, Iterable, List, Callable, TypeVar

from .sessions import Session
from .rules import Rule
from .hashing import Hash, Hashed, HashResolver, HashedBytes, HashedComposite
from .utils import make_nonwritable, Pathable, shorten_text

__version__ = '0.3.0'

_R = TypeVar('_R', bound=Rule)  # type: ignore


def add_source(path: Pathable) -> Callable[[_R], _R]:
    def decorator(rule: _R) -> _R:
        rule.add_extra_arg(lambda: HashedFile(File.from_path(path)))
        return rule

    return decorator


@Rule
async def file_collection(files: List['File']) -> None:
    pass


class FileManager(ABC):
    @abstractmethod
    def store_path(self, path: Path, *, keep: bool) -> 'Hash':
        ...

    @abstractmethod
    def store_bytes(self, content: bytes) -> 'Hash':
        ...

    @abstractmethod
    def get_bytes(self, content_hash: Hash) -> bytes:
        ...

    @abstractmethod
    def target_in(self, path: Path, content_hash: Hash, *, mutable: bool) -> None:
        ...

    @classmethod
    def active(cls) -> Optional['FileManager']:
        fmngr = Session.active().storage.get('file_manager')
        assert not fmngr or isinstance(fmngr, cls)
        return fmngr


class File:
    def __init__(self, path: Path, content: Union[bytes, Hash]):
        assert not path.is_absolute()
        self._path = path
        self._content = content
        if not isinstance(content, bytes):
            fmngr = FileManager.active()
            assert fmngr
            self._fmngr = fmngr

    def __repr__(self) -> str:
        if isinstance(self._content, bytes):
            content = repr(shorten_text(self._content, 20))
        else:
            content = self._content[:6]
        return f'<File path={self._path} content={content}>'

    def __str__(self) -> str:
        return str(self._path)

    @property
    def stem(self) -> str:
        return self._path.stem

    @property
    def path(self) -> Path:
        return self._path

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def content(self) -> Union[bytes, Hash]:
        return self._content

    def read_bytes(self) -> bytes:
        if isinstance(self._content, bytes):
            return self._content
        return self._fmngr.get_bytes(self._content)

    def read_text(self) -> str:
        return self.read_bytes().decode()

    def target_in(self, path: Path, *, mutable: bool = False) -> None:
        target = path / self._path
        if isinstance(self._content, bytes):
            target.write_bytes(self._content)
            if not mutable:
                make_nonwritable(target)
        else:
            self._fmngr.target_in(target, self._content, mutable=mutable)

    @classmethod
    def from_str(cls, path: Pathable, content: Union[str, bytes]) -> 'File':
        path = Path(path)
        if isinstance(content, str):
            content = content.encode()
        fmngr = FileManager.active()
        if fmngr:
            return cls(path, fmngr.store_bytes(content))
        return cls(path, content)

    @classmethod
    def from_path(
        cls, path: Pathable, root: Union[str, Path] = None, *, keep: bool = True
    ) -> 'File':
        path = Path(path)
        relpath = path.relative_to(root) if root else path
        fmngr = FileManager.active()
        if fmngr:
            return cls(relpath, fmngr.store_path(path, keep=keep))
        file = cls(relpath, path.read_bytes())
        if not keep:
            path.unlink()
        return file


class HashedFile(Hashed[File]):
    def __init__(self, file: File):
        self._path = file.path
        if isinstance(file.content, bytes):
            self._content: Optional[HashedBytes] = HashedBytes(file.content)
            self._content_hash = self._content.hashid
        else:
            self._content = None
            self._content_hash = file.content
        Hashed.__init__(self)

    @property
    def spec(self) -> bytes:
        return json.dumps([str(self._path), self._content_hash]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedFile':
        path, content_hash = json.loads(spec)
        path = Path(path)
        fmngr = FileManager.active()
        if fmngr:
            return cls(File(path, content_hash))
        return cls(File(path, cast(HashedBytes, resolve(content_hash)).value))

    @property
    def value(self) -> File:
        return File(
            self._path, self._content.value if self._content else self._content_hash
        )

    @property
    def label(self) -> str:
        return f'./{self._path}'

    @property
    def components(self) -> Iterable['Hashed[object]']:
        if self._content:
            return (self._content,)
        return ()


HashedComposite.type_swaps[File] = HashedFile
