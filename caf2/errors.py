# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


class CafError(Exception):
    pass


class NoActiveSession(CafError):
    pass


class ArgNotInSession(CafError):
    pass


class DependencyCycle(CafError):
    pass


class InvalidFileTarget(CafError):
    pass


class UnknownFile(CafError):
    pass


class FutureNotDone(CafError):
    pass


class FutureHasNoDefault(CafError):
    pass


class TaskHasNotRun(CafError):
    pass


class TaskAlreadyDone(CafError):
    pass


class TaskHookChangedHash(CafError):
    pass


class InvalidJSONObject(CafError):
    pass


class UnhookableResult(CafError):
    pass
