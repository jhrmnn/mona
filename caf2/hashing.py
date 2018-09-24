# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
from typing import Any, NewType, Union, Generic, TypeVar
from typing_extensions import Protocol

_T_co = TypeVar('_T_co', covariant=True)
Hash = NewType('Hash', str)


class Hashed(Protocol, Generic[_T_co]):
    @property
    def hashid(self) -> Hash: ...
    @property
    def label(self) -> str: ...
    @property
    def value(self) -> _T_co: ...


def get_fullname(obj: Any) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'


def hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())
