import pytest  # type: ignore

from caf2 import Session, Rule
from caf2.plugins import Cache, FileManager

from tests.test_dirtask import analysis, calcs
from tests.test_files import calcs2


@pytest.fixture
def db(tmpdir):
    cache = Cache.from_path(tmpdir.join('test.db'))
    yield cache.db
    cache.db.close()


def test_db(db):
    sess = Session([Cache(db)])
    with sess:
        assert sess.eval(analysis(calcs())) == 20
    with sess:
        sess.eval(analysis(calcs()))
        assert len(sess._tasks) == 2


def test_db_files(db, tmpdir):
    sess = Session([Cache(db), FileManager(tmpdir)])
    with sess:
        assert sess.eval(analysis(calcs2())) == 20
    with sess:
        sess.eval(analysis(calcs2()))
        assert len(sess._tasks) == 2


@pytest.mark.filterwarnings("ignore:tasks have never run")
def test_postponed(db):
    cache = Cache(db, eager=False)
    sess = Session([cache])
    with sess:
        sess.eval(analysis(calcs()), task_filter=lambda t: t.label[0] != '/')
        assert len(cache._pending) == 7
        assert len(cache._objects) == 17
        cache.store_pending()
    assert not cache._pending
    assert not cache._objects


@Rule
async def get_object():
    return object()


@pytest.mark.filterwarnings("ignore:tasks have never run")
def test_pickled(db):
    with Session([Cache(db)]) as sess:
        get_object()
    with Session([Cache(db)]) as sess:
        sess.eval(get_object())
    with Session([Cache(db)]) as sess:
        assert type(get_object().value) is object
