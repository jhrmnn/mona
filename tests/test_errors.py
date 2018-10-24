import subprocess
import pickle

import pytest  # type: ignore

from caf import Rule, Session, run_shell
from caf.errors import TaskError, SessionError, CafError

from tests.test_core import identity


def test_no_session():
    with pytest.raises(CafError):
        identity(10)


def test_missing_default():
    with pytest.raises(TaskError):
        with Session():
            identity(identity(1)).call()


def test_missing_default2():
    with pytest.raises(TaskError):
        with Session():
            identity(identity(1)[0]).call()


def test_future_result():
    with pytest.raises(TaskError):
        with Session():
            identity(1).future_result()


def test_future_result2():
    with pytest.raises(TaskError):
        with Session() as sess:
            sess.run_task(identity(1))
            identity(1).future_result()


def test_not_active():
    with pytest.raises(SessionError):
        Session().storage['a'] = 1


def test_no_running_task():
    with pytest.raises(SessionError):
        Session().running_task


@pytest.mark.filterwarnings("ignore:tasks have never run")
def test_fut_not_in_session():
    with pytest.raises(TaskError):
        with Session():
            task = identity(1)
        with Session():
            identity(task[0])


@pytest.mark.filterwarnings("ignore:tasks have never run")
def test_arg_not_in_session():
    with pytest.raises(TaskError):
        with Session():
            task = identity(1)
        with Session():
            identity(task)


def test_dependency_cycle():
    @Rule
    async def f(x):
        return f(x)

    with pytest.raises(CafError):
        with Session() as sess:
            sess.eval(f(1))


def test_unhookable():
    @Rule
    async def f(x):
        return object()

    with pytest.raises(TaskError):
        with Session() as sess:
            task = f(1)
            task.add_hook(lambda x: x)
            sess.eval(task)


def test_invalid_hook():
    with pytest.raises(TaskError):
        with Session() as sess:
            task = identity(1)
            task.add_hook(lambda x: 0)
            sess.eval(task)


def test_resolve_unrun():
    with pytest.raises(TaskError):
        with Session():
            identity(1).resolve()


def test_run_pending():
    with pytest.raises(TaskError):
        with Session() as sess:
            sess.run_task(identity(identity(1)))


def test_run_already_run():
    with Session() as sess:
        with pytest.raises(TaskError):
            sess.run_task(identity(1))
            sess.run_task(identity(1))


def test_no_coroutine():
    with pytest.raises(CafError):
        @Rule
        def f():
            pass


def test_pickled_future():
    with pytest.raises(CafError):
        with Session():
            pickle.dumps(identity())


def test_process_error():
    @Rule
    async def f():
        return await run_shell('<', stderr=subprocess.PIPE)

    with pytest.raises(subprocess.CalledProcessError):
        with Session() as sess:
            sess.eval(f())
