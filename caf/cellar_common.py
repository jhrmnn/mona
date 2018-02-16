# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import sqlite3
import hashlib
from enum import IntEnum

from typing import (
    NewType, NamedTuple, Dict, Tuple, Any, Set, cast, Optional, Union
)

Hash = NewType('Hash', str)
TPath = NewType('TPath', str)
TimeStamp = NewType('TimeStamp', str)


class State(IntEnum):
    CLEAN = 0
    DONE = 1
    DONEREMOTE = 5
    ERROR = -1
    RUNNING = 2
    INTERRUPTED = 3

    @property
    def color(self) -> str:
        return state_colors[self]


state_colors: Dict[State, str] = {
    State.CLEAN: 'normal',
    State.DONE: 'green',
    State.DONEREMOTE: 'cyan',
    State.ERROR: 'red',
    State.RUNNING: 'yellow',
    State.INTERRUPTED: 'blue',
}

sqlite3.register_converter('state', lambda x: State(int(x)))
sqlite3.register_adapter(State, lambda state: cast(int, state.value))


def get_hash(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class Configuration(NamedTuple):
    tasks: Dict[Hash, 'TaskObject']
    targets: Dict[TPath, Hash]
    inputs: Dict[Hash, Union[str, bytes]]
    labels: Dict[Hash, TPath]


class TaskObject:
    def __init__(self,
                 execid: str,
                 command: str,
                 inputs: Dict[str, Hash] = None,
                 symlinks: Dict[str, str] = None,
                 childlinks: Dict[str, Tuple[Hash, str]] = None,
                 outputs: Optional[Dict[str, Hash]] = None) -> None:
        self.execid = execid
        self.command = command
        self.inputs = inputs or {}
        self.symlinks = symlinks or {}
        self.childlinks = childlinks or {}
        self.outputs = outputs

    def __repr__(self) -> str:
        return (
            f'<TaskObj execd={self.execid!r} command={self.command!r} '
            f'inputs={self.inputs!r} symlinks={self.symlinks!r} '
            f'childlinks={self.childlinks!r} outputs={self.outputs!r}>'
        )

    def asdict_v2(self, with_outputs: bool = False) -> Dict[str, Any]:
        inputs = cast(Dict[str, str], self.inputs.copy())
        for name, target in self.symlinks.items():
            inputs[name] = '>' + target
        for name, (hs, target) in self.childlinks.items():
            inputs[name] = f'@{hs}/{target}'
        obj = {'command': self.command, 'inputs': inputs}
        if self.outputs is not None:
            obj['outputs'] = self.outputs
        return obj

    @property
    def data(self) -> bytes:
        return json.dumps(self.asdict_v2(), sort_keys=True).encode()

    @property
    def children(self) -> Set[Hash]:
        return set(hs for hs, _ in self.childlinks.values())

    @property
    def hashid(self) -> Hash:
        return get_hash(self.data)

    @classmethod
    def from_data(cls,
                  execid: str,
                  inp: bytes,
                  out: Optional[bytes] = None) -> 'TaskObject':
        obj: Dict[str, Any] = json.loads(inp)
        inputs: Dict[str, Hash] = {}
        symlinks: Dict[str, str] = {}
        childlinks: Dict[str, Tuple[Hash, str]] = {}
        for name, target in obj['inputs'].items():
            if target[0] == '>':
                symlinks[name] = target[1:]
            elif target[0] == '@':
                hs, target = target[1:].split('/', 1)
                childlinks[name] = (Hash(hs), target)
            else:
                inputs[name] = Hash(target)
        outputs: Dict[str, Hash] = json.loads(out) if out else None
        return cls(
            execid, obj['command'], inputs, symlinks, childlinks, outputs
        )
