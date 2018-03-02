# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import argparse
from argparse import ArgumentParser
from collections import OrderedDict

from typing import (
    Any, Callable, TypeVar, List, Union, Dict, Tuple, Iterable, Optional
)

_F = TypeVar('_F', bound=Callable[..., Any])

CliDef = Iterable[Tuple[str, Union[Callable[..., Any], List[Tuple[str, Any]]]]]


class Arg:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs


class CLIError(Exception):
    def __init__(self, parser: ArgumentParser, msg: str) -> None:
        self.parser = parser
        self.msg = msg

    def reraise(self) -> None:
        ArgumentParser.error(self.parser, self.msg)


class ThrowingArgumentParser(ArgumentParser):
    def error(self, msg: str) -> None:
        raise CLIError(self, msg)


class CLI:
    def __init__(self) -> None:
        self.parser = ThrowingArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        self._commands: Optional[Dict[str, Any]] = OrderedDict()
        self._func_register: Dict[Callable[..., Any], List[Arg]] = {}

    def parse(self, argv: List[str]) -> Dict[str, Any]:
        if self._commands:
            self._add_commands(self.parser, self._commands.items())
            self._commands = None
        if not argv:
            raise CLIError(self.parser, self.parser.format_help().strip())
        return {
            k: v for k, v in vars(self.parser.parse_args(argv)).items()
            if v is not None
        }

    def run(self, *args: Any, argv: List[str]) -> Any:
        kwargs = self.parse(argv)
        func = kwargs.pop('func')
        return func(*args, **kwargs)

    def add_command(self, func: Callable[..., Any], cli: List[Arg] = None,
                    name: str = None, group: str = None) -> None:
        assert self._commands is not None
        self._func_register[func] = cli or []
        if not name:
            if '_' in func.__name__:
                group, name = func.__name__.split('_', 1)
            else:
                name = func.__name__
        if not group:
            self._commands[name] = func
        else:
            self._commands.setdefault(group, []).append((name, func))

    def command(self, cli: List[Arg] = None, name: str = None, group: str = None
                ) -> Callable[[_F], _F]:
        def decorator(func: _F) -> _F:
            self.add_command(func, cli, name, group)
            return func
        return decorator

    def _add_commands(self, parser: ArgumentParser, clidef: CliDef) -> None:
        rows = []
        for name, item in clidef:
            if isinstance(item, list):
                rows.append((
                    name,
                    '-> ' + ', '.join(subname for subname, _ in item)
                ))
            else:
                rows.append((name, item.__doc__ or '?'))
        maxlen = max(len(name) for name, _ in rows)
        subparsers = parser.add_subparsers(
            metavar='<command>',
            help='Command to run',
            title='commands',
            description='\n'.join(
                f'{name:<{maxlen}}   {desc}' for name, desc in rows
            ),
        )
        for name, item in clidef:
            subparser = subparsers.add_parser(
                name,
                formatter_class=parser.formatter_class  # type: ignore
            )
            if isinstance(item, list):
                self._add_commands(subparser, item)
            else:
                for arg in self._func_register[item]:
                    subparser.add_argument(*arg.args, **arg.kwargs)
                subparser.set_defaults(func=item)
