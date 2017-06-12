# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from argparse import ArgumentParser

from typing import (  # noqa
    Any, Callable, TypeVar, List, NewType, Union, Dict, Tuple, Optional,
    Iterable
)
from argparse import _SubParsersAction  # noqa


_T = TypeVar('_T')
_F = TypeVar('_F', bound=Callable[..., Any])


class Arg:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs


def define_cli(cli: List[Arg] = None) -> Callable[[_F], _F]:
    def decorator(func: _F) -> _F:
        func.__cli__ = cli or []  # type: ignore
        return func
    return decorator


CliDef = List[Tuple[str, Union[Callable, List[Tuple[str, Any]]]]]


def _add_commands(parser: ArgumentParser, clidef: CliDef) -> None:
    subparsers = parser.add_subparsers()
    for name, item in clidef:
        subparser = subparsers.add_parser(name)
        if isinstance(item, list):
            _add_commands(subparser, item)
        else:
            for arg in item.__cli__:  # type: ignore
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
        self.parser = ThrowingArgumentParser()
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
