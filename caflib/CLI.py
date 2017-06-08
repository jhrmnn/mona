# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from textwrap import dedent
from collections import OrderedDict
import sys

from typing import (  # noqa
    Callable, Type, Union, Tuple, Dict, List, Any, cast, Optional, Iterable,
    TYPE_CHECKING
)
if TYPE_CHECKING:
    from mypy_extensions import NoReturn
else:
    NoReturn = None

try:
    from docopt import docopt
except ImportError:
    print('Error: Cannot import docopt')
    sys.exit(1)

ArgSpec = Union[str, Tuple[str, Type]]
Trigger = Tuple[str, ...]


class Command:
    def __init__(self, func: Callable, mapping: Dict[str, ArgSpec],
                 name: str = None, doc: str = None) -> None:
        self.name = name or func.__name__
        self._func = func
        self._mapping = mapping
        doc = doc or func.__doc__
        assert doc
        self._doc = doc

    def __repr__(self) -> str:
        return f'<Command "{self.name}">'

    def __str__(self) -> str:
        s = dedent(self._doc).rstrip()
        firstline, s = s.split('\n', 1)
        if firstline:
            s = firstline + '\n' + s
        return s

    def __format__(self, fmt: str) -> str:
        if not fmt:
            return str(self)
        elif fmt == 'header':
            return str(self).split('\n', 1)[0]
        else:
            raise ValueError('Invalid format specifier')

    def parse(self, argv: List[str], *clis: 'CLI') -> Dict[str, Any]:
        command = ' '.join(cli.name for cli in clis) + ' ' + self.name
        doc = str(self).replace('<command>', command)
        return docopt(doc, argv=argv[1:])

    def __call__(self, argv: List[str], *clis: 'CLI') -> Any:
        args = self.parse(argv, *clis)
        kwargs = {}
        for key, name in self._mapping.items():
            if isinstance(name, tuple):
                name, func = name
                if args[name] is not None:
                    if isinstance(func, str):
                        for ctx in clis:
                            if hasattr(ctx, func):
                                kwargs[key] = getattr(ctx, func)(args[name])
                                break
                        else:
                            raise RuntimeError(
                                f'{self!r} has no context for "{func}"'
                            )
                    else:
                        kwargs[key] = func(args[name])
                else:
                    kwargs[key] = None
            else:
                kwargs[key] = args[name]
        return self._func(*clis, **kwargs)


class CLIExit(SystemExit):
    pass


class CLIMeta(type):
    def __new__(cls, name: str, bases: Tuple[Type], namespace: Dict[str, Any]) -> Any:
        cls = type.__new__(cls, name, bases, namespace)
        if cls.__base__ == object:
            cls.commands: Dict[Trigger, Command] = OrderedDict()
        else:
            cls.commands = cls.__base__.commands.copy()
        return cls


def bind_function(obj: Union['CLI', Type['CLI']], func: Callable,
                  name: str = None, triggers: List[str] = None,
                  mapping: Dict[str, ArgSpec] = None) -> Command:
    command = Command(func, mapping or {}, name=name)
    obj.commands[(command.name.strip('_'),)] = command
    for trigger in triggers or []:
        obj.commands[tuple(trigger.split())] = command
    return command


class CLI(metaclass=CLIMeta):

    def __init__(self, name: str, header: str = None) -> None:
        self.name = name
        self.header = header
        self.commands: Dict[Tuple[str, ...], Union[Command, 'CLI']] \
            = self.__class__.commands.copy()  # type: ignore

    def __repr__(self) -> str:
        return f'<CLI "{self.name}">'

    def __str__(self) -> str:
        parts = []
        for part in ['header', 'usage', 'options', 'commands']:
            try:
                parts.append(format(self, part))
            except ValueError:
                pass
        return '\n\n'.join(parts)

    def __format__(self, fmt: str) -> str:
        if not fmt:
            return str(self)
        elif fmt == 'header':
            if self.header:
                return self.header
            else:
                raise ValueError('Invalid format specifier')
        elif fmt == 'usage':
            usage = """\
            Usage:
                <command> COMMAND [ARGS...]
            """.rstrip()
            return dedent(usage)
        elif fmt == 'commands':
            maxwidth = max(len(trigger) for trigger, *_ in self.commands)
            commands = [
                f'    {trigger:{maxwidth}}    {command:header}'
                for (trigger, *other), command in self.commands.items()
                if not other
            ]
            return 'Commands:\n{}'.format('\n'.join(commands))
        elif fmt == 'short':
            return f'{self:usage}\n\n{self:commands}'
        else:
            raise ValueError('Invalid format specifier')

    def find_command(self, argv: List[str], *clis: 'CLI') -> Optional[Trigger]:
        offset = len(clis)+1
        nargv = len(argv)-offset
        maxlen = min(max(map(len, self.commands.keys())), nargv)
        trigger = None
        for tlen in range(maxlen, 0, -1):
            for trigger in self.commands:
                if trigger == tuple(argv[offset:offset+tlen]):
                    break
            else:
                trigger = None
            if trigger:
                break
        return trigger

    def parse(self, argv: List[str], *clis: 'CLI') -> Dict[str, Any]:
        trigger = self.find_command(argv, *clis)
        if not trigger:
            self.exit(clis)
        clis += (self,)
        return self.commands[trigger].parse(argv, *clis)

    def __call__(self, argv: List[str], *clis: 'CLI') -> Any:
        trigger = self.find_command(argv, *clis)
        if not trigger:
            self.exit(clis)
        clis += (self,)
        return self.commands[trigger](argv, *clis)

    def exit(self, clis: Tuple['CLI', ...]) -> NoReturn:
        clinames = ' '.join(cli.name for cli in clis + (self,))
        raise CLIExit(format(self, 'short').replace('<command>', clinames))

    def add_command(self, **kwargs: Any) -> Callable[[Callable], Command]:
        def decorator(func: Callable) -> Command:
            return bind_function(self, func, **kwargs)
        return decorator

    @classmethod
    def command(cls, **kwargs: Any) -> Callable[[Callable], Command]:
        def decorator(func: Callable) -> Command:
            return bind_function(cls, func, **kwargs)
        return decorator


@CLI.command(mapping=dict(command='COMMAND'))
def help(*clis: CLI, command: str) -> None:
    """
    Print help for individual commands.

    Usage:
        <command> [COMMAND]
    """
    cli = clis[-1]
    clinames = ' '.join(cli.name for cli in clis)
    if command:
        trigger = (command,)
        if trigger in cli.commands:
            cmd = cli.commands[trigger]
            print(str(cmd).replace('<command>', clinames + ' ' + cmd.name))
        else:
            cli.exit(clis)
    else:
        print(str(cli).replace('<command>', clinames))
