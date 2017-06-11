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


def _add_commands(parser: ArgumentParser, cmds: CliDef) -> None:
    subparsers = parser.add_subparsers()
    for name, cmd in cmds:
        subparser = subparsers.add_parser(name)
        if isinstance(cmd, list):
            _add_commands(subparser, cmd)
        else:
            for arg in cmd.__cli__:  # type: ignore
                subparser.add_argument(*arg.args, **arg.kwargs)
            subparser.set_defaults(func=cmd)


class CLI:
    def __init__(self, cmds: CliDef, args: Iterable[Any] = None) -> None:
        self.parser = ArgumentParser()
        self.args = tuple(args) if args else ()
        _add_commands(self.parser, cmds)

    def parse(self, args: List[str] = None) -> Dict[str, Any]:
        return {
            k: v for k, v in vars(self.parser.parse_args(args)).items() if v
        }

    def run(self, args: List[str] = None) -> Any:
        kwargs = self.parse(args)
        func = kwargs.pop('func')
        return func(*self.args, **kwargs)
