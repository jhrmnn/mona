# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import ast
import inspect
import json
import os
import sys
from itertools import chain, dropwhile
from pathlib import Path
from textwrap import dedent
from types import CodeType, ModuleType
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from .errors import CompositeError, HashingError
from .hashing import Hash, Hashed, HashedComposite, hash_text
from .utils import get_fullname

__all__ = ()

_T = TypeVar('_T')

# Travis duplicates some stdlib modules in virtualenv
_stdlib_paths = [str(Path(m.__file__).parent) for m in [os, ast]]
_cache: Dict[Callable[..., Any], Hash] = {}


def is_stdlib(mod: ModuleType) -> bool:
    return any(mod.__file__.startswith(p) for p in _stdlib_paths)


def version_of(mod: ModuleType) -> Optional[str]:
    parts = mod.__name__.split('.')
    for n in range(len(parts), 0, -1):
        mod = sys.modules['.'.join(parts[:n])]
        try:
            return cast(str, getattr(mod, '__version__'))
        except AttributeError:
            pass
    return None


def hash_function(func: Callable[..., Any]) -> Hash:
    try:
        return _cache[func]
    except KeyError:
        pass
    ast_code = ast_code_of(func)
    hashed_globals = hashed_globals_of(func)
    spec = json.dumps({'ast_code': ast_code, 'globals': hashed_globals}, sort_keys=True)
    return _cache.setdefault(func, hash_text(spec))


def ast_code_of(func: Callable[..., Any]) -> str:
    lines = dedent(inspect.getsource(func)).split('\n')
    lines = list(dropwhile(lambda l: l[0] == '@', lines))
    code = '\n'.join(lines)
    module = ast.parse(code)
    assert len(module.body) == 1
    assert isinstance(module.body[0], (ast.AsyncFunctionDef, ast.FunctionDef))
    for node in ast.walk(module):
        remove_docstring(node)
    func_node = module.body[0]
    func_node.name = ''  # clear function's name
    return ast.dump(func_node, annotate_fields=False)


def remove_docstring(node: ast.AST) -> None:
    classes = ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef, ast.Module
    if not isinstance(node, classes):
        return
    if not (node.body and isinstance(node.body[0], ast.Expr)):
        return
    docstr = node.body[0].value
    if isinstance(docstr, ast.Str):
        node.body.pop(0)


def hashed_globals_of(func: Callable[..., Any]) -> Dict[str, str]:
    closure_vars = getclosurevars(func)
    items = chain(closure_vars.nonlocals.items(), closure_vars.globals.items())
    hashed_globals: Dict[str, str] = {}
    for name, obj in items:
        if hasattr(obj, '_func_hash'):
            hashid = (
                obj._func_hash()
                if getattr(obj, 'corofunc', None) is not func
                else 'self'
            )
            hashed_globals[name] = f'func_hash:{hashid}'
            continue
        if isinstance(obj, Hashed):
            hashed_globals[name] = f'hashed:{obj.hashid}'
            continue
        if inspect.isclass(obj) or inspect.isfunction(obj) or inspect.ismodule(obj):
            if inspect.ismodule(obj):
                mod = obj
                fullname = obj.__name__
            else:
                mod = sys.modules[obj.__module__]
                fullname = get_fullname(obj)
            if is_stdlib(mod):
                hashed_globals[name] = f'{fullname}(stdlib)'
                continue
            version = version_of(mod)
            if version:
                hashed_globals[name] = f'{fullname}({version})'
                continue
            if inspect.isfunction(obj):
                hashid = hash_function(obj) if obj is not func else 'self'
                hashed_globals[name] = f'function:{hashid}'
                continue
        try:
            hashid = HashedComposite.from_object(obj).hashid
        except CompositeError:
            pass
        else:
            hashed_globals[name] = f'composite:{hashid}'
            continue
        raise HashingError(f'In {func} cannot hash global {name} = {obj!r}')
    return hashed_globals


# adapted function from stdlib which parses closures in code consts as well
# see https://bugs.python.org/issue34947
def getclosurevars(func: Callable[..., Any]) -> inspect.ClosureVars:
    code = func.__code__
    nonlocal_vars = {
        name: cell.cell_contents
        for name, cell in zip(code.co_freevars, func.__closure__ or ())  # type: ignore
    }
    global_ns = func.__globals__  # type: ignore
    builtin_ns = global_ns['__builtins__']
    if inspect.ismodule(builtin_ns):
        builtin_ns = builtin_ns.__dict__
    global_vars = {}
    builtin_vars = {}
    unbound_names = set()
    codes = [code]
    while codes:
        code = codes.pop()
        for const in code.co_consts:
            if isinstance(const, CodeType):
                codes.append(const)
        for name in code.co_names:
            try:
                global_vars[name] = global_ns[name]
            except KeyError:
                try:
                    builtin_vars[name] = builtin_ns[name]
                except KeyError:
                    unbound_names.add(name)
    return inspect.ClosureVars(nonlocal_vars, global_vars, builtin_vars, unbound_names)
