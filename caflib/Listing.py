# import os
# from pathlib import Path
# import math
#
#
# class Range:
#     def __init__(self, a, b):
#         self.a = a
#         self.b = b
#
#     def __contains__(self, x):
#         return (self.a is None or x >= self.a) and (self.b is None or x <= self.b)
#
#
# def walk(top, **kwargs):
#     topdepth = top.count(os.path.sep)
#     if Path(top).is_dir():
#         yield '.', [top], [], 0
#     elif Path(top).is_file():
#         yield '.', [], [top], 0
#     for curpathname, dirnames, filenames in os.walk(top, **kwargs):
#         curdepth = curpathname.count(os.path.sep)
#         yield curpathname, dirnames, filenames, curdepth-topdepth+1
#
#
# def find(*roots, follow=False, filterfile=None, filterdir=None, filterpath=None,
#          prunedir=None, prunepath=None, onlyfile=False, onlydir=False,
#          depth=None, mindepth=None, maxdepth=None):
#     if maxdepth is None:
#         maxdepth = math.inf
#     if mindepth is None:
#         mindepth = 0
#     if depth is not None:
#         if isinstance(depth, int):
#             depth = [depth]
#         maxdepth = max(depth)
#     else:
#         depth = Range(mindepth, maxdepth)
#     for root in map(str, roots):
#         for curpathname, dirnames, filenames, curdepth in walk(root, followlinks=follow):
#             ldirnames = list(dirnames)
#             curpath = Path(curpathname)
#             if not onlydir and curdepth in depth:
#                 if filterfile:
#                     filenames = filter(filterfile, filenames)
#                 yield from (curpath/fname for fname in filenames)
#             if filterdir:
#                 ldirnames = filter(filterdir, ldirnames)
#             if filterpath:
#                 ldirnames = [dname for dname in ldirnames if filterpath(curpath/dname)]
#             if not onlyfile and curdepth in depth:
#                 yield from (curpath/dname for dname in ldirnames)
#             if curdepth >= maxdepth:
#                 del dirnames[:]
#             if prunedir:
#                 dirnames[:] = filter(prunedir, dirnames)
#             if prunepath:
#                 dirnames[:] = [dname for dname in dirnames if prunepath(curpath/dname)]
#
#
# def find_tasks(*roots, sealed=False, stored=False, error=False, unsealed=False,
#                maxdepth=None, follow=True):
#     if sealed:
#         def filterpath(p):
#             return (p/'.caf/seal').is_file() or (p/'.caf/remote_seal').is_file()
#     elif unsealed:
#         def filterpath(p):
#             return (p/'.caf/lock').is_file() and not (p/'.caf/seal').is_file() \
#                 and not (p/'.caf/remote_seal').is_file()
#     elif stored:
#         def filterpath(p):
#             return (p/'.caf/lock').is_file()
#     elif error:
#         def filterpath(p):
#             return (p/'.caf/error').is_file()
#     else:
#         def filterpath(p):
#             return (p/'.caf').is_dir()
#     return find(*roots,
#                 filterpath=filterpath,
#                 follow=follow,
#                 onlydir=True,
#                 maxdepth=maxdepth)
