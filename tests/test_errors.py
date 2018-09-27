import pytest  # type: ignore

from caf2 import Rule, Session
from caf2.sessions import NoActiveSession, ArgNotInSession, DependencyCycle

from tests.test_core import identity


def test_no_session():
    with pytest.raises(NoActiveSession):
        identity(10)


@pytest.mark.filterwarnings("ignore:tasks were never run")
def test_fut_not_in_session():
    with pytest.raises(ArgNotInSession):
        with Session():
            task = identity(1)
        with Session():
            identity(task[0])


@pytest.mark.filterwarnings("ignore:tasks were never run")
def test_arg_not_in_session():
    with pytest.raises(ArgNotInSession):
        with Session():
            task = identity(1)
        with Session():
            identity(task)


def test_dependency_cycle():
    @Rule
    def f(x):
        return f(x)

    with pytest.raises(DependencyCycle):
        with Session() as sess:
            sess.eval(f(1))
