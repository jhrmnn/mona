# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import numpy as np


class ArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if obj is np.nan:
            return None
        try:
            return obj.tolist()
        except AttributeError:
            return super().default(obj)
