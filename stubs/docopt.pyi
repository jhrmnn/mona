from typing import List, Dict, Any


def docopt(doc: str, argv: List[str] = None, help: bool = True,
           version: str = None, options_first: bool = False) \
                -> Dict[str, Any]: ...


class DocoptExit(Exception): ...
