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
