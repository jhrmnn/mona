import pytest  # type: ignore

from caf2.plugins import CachedSession
from caf2.plugins.cache import init_cafdb

from tests.test_dirtask import analysis, calcs


@pytest.fixture
def db(tmpdir):
    conn = init_cafdb(tmpdir.join('test.db'))
    yield conn
    conn.close()


def test_db(db):
    sess = CachedSession(db)
    with sess:
        assert sess.eval(analysis(calcs())) == 20
    with sess:
        sess.eval(analysis(calcs()))
        assert len(sess._tasks) == 2
