import subprocess

import pytest  # type: ignore

from caf2 import Rule, Session, run_shell
from caf2.errors import NoActiveSession, ArgNotInSession, DependencyCycle, \
    UnhookableResult, TaskHookChangedHash, FutureHasNoDefault, \
    TaskAlreadyDone, TaskHasNotRun, TaskHasAlreadyRun, TaskNotReady, \
    TaskFunctionNotCoroutine, NoRunningTask, SessionNotActive

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


def test_not_active():
    with pytest.raises(SessionNotActive):
        Session().storage['a'] = 1


def test_no_running_task():
    with pytest.raises(NoRunningTask):
        Session().running_task


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
    async def f(x):
        return f(x)

    with pytest.raises(DependencyCycle):
        with Session() as sess:
            sess.eval(f(1))


def test_unhookable():
    @Rule
    async def f(x):
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


def test_resolve_unrun():
    with pytest.raises(TaskHasNotRun):
        with Session():
            identity(1).resolve()


def test_run_pending():
    with pytest.raises(TaskNotReady):
        with Session() as sess:
            sess.run_task(identity(identity(1)))


def test_run_already_run():
    with Session() as sess:
        with pytest.raises(TaskHasAlreadyRun):
            sess.run_task(identity(1))
            sess.run_task(identity(1))


def test_no_coroutine():
    @Rule
    def f():
        pass

    with Session():
        with pytest.raises(TaskFunctionNotCoroutine):
            f()


def test_process_error():
    @Rule
    async def f():
        return await run_shell('<', stderr=subprocess.PIPE)

    with pytest.raises(subprocess.CalledProcessError):
        with Session() as sess:
            sess.eval(f())
