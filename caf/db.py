# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sqlite3
from abc import ABC
from typing import Iterable, Any

from .Logging import no_cafdir


class WithDB(ABC):
    def init_db(self, path: str) -> None:
        try:
            self._db = sqlite3.connect(
                path,
                detect_types=sqlite3.PARSE_COLNAMES,
                timeout=30.0,
            )
        except sqlite3.OperationalError:
            no_cafdir()

    def execute(self, sql: str, *parameters: Iterable[Any]) -> sqlite3.Cursor:
        return self._db.execute(sql, *parameters)

    def executemany(self, sql: str, *seq_of_parameters: Iterable[Iterable[Any]]) -> sqlite3.Cursor:
        return self._db.executemany(sql, *seq_of_parameters)

    def commit(self) -> None:
        self._db.commit()
