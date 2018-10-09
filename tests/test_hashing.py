import pytest  # type: ignore

from caf2 import Rule
from caf2.errors import HashingError


def test_docstring():
    @Rule
    async def f():
        return 1

    @Rule
    async def g():
        """docstring"""
        return 1

    assert f._func_hash() == g._func_hash()


def test_whitespace():
    @Rule
    async def f():

        return 1

    @Rule
    async def g():
        return 1  # comment

    assert f._func_hash() == g._func_hash()


def test_different():
    @Rule
    async def f():
        return 1

    @Rule
    async def g():
        return 2

    assert f._func_hash() != g._func_hash()


def test_constant():
    dct = {'a': 1}

    @Rule
    async def f():
        return dct

    f._func_hash()


obj = object()


def test_unhashable():
    @Rule
    async def f():
        return obj

    with pytest.raises(HashingError):
        f._func_hash()
