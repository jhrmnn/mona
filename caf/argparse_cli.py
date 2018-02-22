# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import argparse
from argparse import ArgumentParser
import functools

from typing import Any, Callable, TypeVar, List, Union, Dict, Tuple


_T = TypeVar('_T')
_F = TypeVar('_F', bound=Callable[..., Any])


class Arg:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs


_func_register: Dict[Callable[..., Any], List[Arg]] = {}


def define_cli(cli: List[Arg] = None) -> Callable[[_F], _F]:
    def decorator(func: _F) -> _F:
        _func_register[func] = cli or []
        return func
    return decorator


def partial(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    newfunc = functools.partial(func, *args, **kwargs)
    newfunc.__doc__ = func.__doc__
    if func in _func_register:
        _func_register[newfunc] = _func_register[func]
    return newfunc


CliDef = List[Tuple[str, Union[Callable[..., Any], List[Tuple[str, Any]]]]]


def _add_commands(parser: ArgumentParser, clidef: CliDef) -> None:
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
            _add_commands(subparser, item)
        else:
            for arg in _func_register[item]:
                subparser.add_argument(*arg.args, **arg.kwargs)
            subparser.set_defaults(func=item)


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
    def __init__(self, cmds: CliDef) -> None:
        self.parser = ThrowingArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        _add_commands(self.parser, cmds)

    def parse(self, argv: List[str] = None) -> Dict[str, Any]:
        return {
            k: v for k, v in vars(self.parser.parse_args(argv)).items() if v
        }

    def run(self, *args: Any, argv: List[str] = None) -> Any:
        kwargs = self.parse(argv)
        if not kwargs:
            return
        func = kwargs.pop('func')
        return func(*args, **kwargs)
