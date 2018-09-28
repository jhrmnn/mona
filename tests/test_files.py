import shutil
from pathlib import Path

import pytest  # type: ignore

from caf2 import Rule, Session, FileManager
from caf2.rules import dir_task
from caf2.hashing import HashedBytes
from caf2.files import StoredHashedBytes
from caf2.errors import UnknownFile, UnrecognizedInput, DupliciteInputFile

from tests.test_dirtask import calcs, analysis


@pytest.fixture(scope='module')
def datafile():
    path = Path('data')
    path.write_text(str(2))
    try:
        yield path
    finally:
        path.unlink()


@Rule
def calcs2():
    return [[
        dist,
        dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            [('data', str(dist).encode())],
            {'input': 'data'},
            label=f'/calcs/dist={dist}'
        ).get('STDOUT', b'0')
    ] for dist in range(5)]


def test_hashing(tmpdir):
    def run(calcs, fmngr=None):
        with Session() as sess:
            if fmngr:
                fmngr(sess)
            sess.run_task(calcs())
            fut = calcs().future_result()
            sess.eval(calcs())
            task = fut.resolve()[0][1].task
            output = task.resolve().resolve()['STDOUT']
            return task, output

    fmngr = FileManager(tmpdir)
    without_fmngr = run(calcs)
    with_fmngr = run(calcs, fmngr)
    with Session() as sess:
        fmngr(sess)
        task = dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            [('data', str(0).encode())],
            {'input': 'data'},
        )
        sess.run_task(task)
        alt_input = task, task.resolve().resolve()['STDOUT']
    alt_input = run(calcs2, fmngr)
    assert len(fmngr._cache) == 12
    assert len(list(Path(tmpdir).glob('*/*'))) == 12
    assert isinstance(without_fmngr[1], HashedBytes)
    assert isinstance(with_fmngr[1], StoredHashedBytes)
    assert isinstance(alt_input[1], StoredHashedBytes)
    assert without_fmngr[0].hashid == with_fmngr[0].hashid
    assert without_fmngr[0].hashid == alt_input[0].hashid
    assert without_fmngr[1].hashid == with_fmngr[1].hashid
    assert without_fmngr[1].hashid == alt_input[1].hashid


def test_missing_file(tmpdir):
    fmngr = FileManager(tmpdir)
    with pytest.raises(UnknownFile):
        with Session() as sess:
            fmngr(sess)
            sess.run_task(calcs())
            shutil.rmtree(tmpdir)
            fmngr._cache.clear()
            sess.eval(calcs())


def test_access(tmpdir):
    fmngr = FileManager(tmpdir)
    with Session() as sess:
        fmngr(sess)
        sess.run_task(calcs())
        fut = calcs().future_result()
        sess.eval(calcs())
        task = fut.resolve()[0][1].task
        output = task.resolve().resolve()['STDOUT']
        assert int(output.value) == 0
        shutil.rmtree(tmpdir)
        fmngr._cache.clear()
        with pytest.raises(UnknownFile):
            output.value


def test_alt_input(datafile, tmpdir):
    def create_task():
        return dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            ['data'],
            {'input': 'data'},
        )
    fmngr = FileManager(tmpdir)
    with Session() as sess:
        fmngr(sess)
        sess.run_task(create_task())
        int(create_task().value['STDOUT']) == 4


def test_alt_input2(datafile, tmpdir):
    fmngr = FileManager(tmpdir)
    with Session() as sess:
        fmngr(sess)
        assert int(sess.run_task(dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            [Path('data')],
            {'input': 'data'},
        )).value['STDOUT']) == 4


def test_alt_input3(tmpdir):
    tmpdir = Path(tmpdir)
    (tmpdir/'data').write_text('2')
    fmngr = FileManager(tmpdir)
    with Session() as sess:
        fmngr(sess)
        with pytest.raises(UnrecognizedInput):
            dir_task(b'', [object()])
        with pytest.raises(DupliciteInputFile):
            dir_task(b'', [('data', b''), ('data', b'')])
        with pytest.raises(DupliciteInputFile):
            dir_task(b'', [('data', b'')], {'data': 'data'})