from caflib.Core import Calculation
import json


class MBDCalculation(Calculation):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.command = 'run_aims'

    def prepare(self):
        super().prepare()
        with open('input.json', 'w') as f:
            json.dump(self.kwargs, f)
