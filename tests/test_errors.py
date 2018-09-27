import pytest  # type: ignore

from caf2 import Rule, Session
from caf2.errors import NoActiveSession, ArgNotInSession, DependencyCycle, \
    UnhookableResult, TaskHookChangedHash, FutureHasNoDefault, TaskAlreadyDone, \
    TaskHasNotRun

from tests.test_core import identity


def test_no_session():
    with pytest.raises(NoActiveSession):
        identity(10)


def test_missing_default():
    with pytest.raises(FutureHasNoDefault):
        with Session():
            identity(identity(1)).call()


def test_missing_default2():
    with pytest.raises(FutureHasNoDefault):
        with Session():
            identity(identity(1)[0]).call()


def test_future_result():
    with pytest.raises(TaskHasNotRun):
        with Session():
            identity(1).future_result()


def test_future_result2():
    with pytest.raises(TaskAlreadyDone):
        with Session() as sess:
            sess.run_task(identity(1))
            identity(1).future_result()


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


def test_unhookable():
    @Rule
    def f(x):
        return object()

    with pytest.raises(UnhookableResult):
        with Session() as sess:
            task = f(1)
            task.add_hook(lambda x: x)
            sess.eval(task)


def test_invalid_hook():
    with pytest.raises(TaskHookChangedHash):
        with Session() as sess:
            task = identity(1)
            task.add_hook(lambda x: 0)
            sess.eval(task)
