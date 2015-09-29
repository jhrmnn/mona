import subprocess
from pathlib import Path


def find_program(cmd):
    return Path(subprocess.check_output(['which', cmd]).decode().strip()).resolve()
