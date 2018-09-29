import pytest  # type: ignore

from caf2 import Session
from caf2.plugins import Cache

from tests.test_dirtask import analysis, calcs


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
