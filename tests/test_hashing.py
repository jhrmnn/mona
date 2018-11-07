import pytest  # type: ignore

from mona.errors import HashingError
from mona.pyhash import hash_function


def test_docstring():
    async def f():
        return 1

    async def g():
        """Docstring."""
        return 1

    assert hash_function(f) == hash_function(g)


def test_whitespace():
    async def f():

        return 1

    async def g():
        return 1  # comment

    assert hash_function(f) == hash_function(g)


def test_different():
    async def f():
        return 1

    async def g():
        return 2

    assert hash_function(f) != hash_function(g)


def test_constant():
    dct = {'a': 1}

    async def f():
        1
        return dct

    h1 = hash_function(f)
    dct['a'] = 2

    async def f():
        1
        return dct

    h2 = hash_function(f)
    assert h1 != h2


obj = object()


def test_unhashable():
    async def f():
        return obj

    with pytest.raises(HashingError):
        hash_function(f)
