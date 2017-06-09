# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import chain, product, repeat
import os
from io import StringIO
from collections import defaultdict
import numpy as np  # type: ignore

from typing import (  # noqa
    List, Tuple, DefaultDict, Iterator, IO, Sized, Iterable, Union, Dict, Any,
    Optional
)

from caflib.Tools import geomlib


specie_data = geomlib.Atom.data
bohr = geomlib.bohr


Vec = Tuple[float, float, float]


_string_cache: Dict[Tuple[str, Vec, str], str] = {}


class Molecule(Sized, Iterable):
    def __init__(self, species: List[str], coords: List[Vec],
                 constrains: Dict[Union[int, str], List[str]] = None) -> None:
        self.species = species
        self.coords = coords
        self.numbers: List[int] = [int(specie_data[sp]['number']) for sp in species]
        self.constrains: Dict[Union[str, int], List[str]] = defaultdict(list)
        if constrains:
            self.constrains.update(constrains)

    def __repr__(self) -> str:
        return "<{} '{}'>".format(self.__class__.__name__, self.formula)

    @property
    def xyz(self) -> np.ndarray:
        return np.array(self.coords)

    @property
    def formula(self) -> str:
        counter = DefaultDict[str, int](int)
        for specie in self.species:
            counter[specie] += 1
        return ''.join(
            f'{sp}{n if n > 1 else ""}' for sp, n in sorted(counter.items())
        )

    def __iter__(self) -> Iterator[Tuple[str, np.ndarray]]:
        yield from zip(self.species, self.coords)

    def __len__(self) -> int:
        return len(self.species)

    def __format__(self, fmt: str) -> str:
        fp = StringIO()
        self.dump(fp, fmt)
        return fp.getvalue()

    dumps = __format__

    def dump(self, f: IO[str], fmt: str) -> None:
        if fmt == '':
            f.write(repr(self))
        elif fmt == 'xyz':
            f.write('{}\n'.format(len(self)))
            f.write('Formula: {}\n'.format(self.formula))
            for specie, coord in self:
                f.write('{:>2} {}\n'.format(
                    specie, ' '.join('{:15.8}'.format(x) for x in coord)
                ))
        elif fmt == 'aims':
            for i, atom in enumerate(self):
                specie, r = atom
                key = atom + (fmt,)
                try:
                    f.write(_string_cache[key])
                except KeyError:
                    s = f'atom {r[0]:15.8f} {r[1]:15.8f} {r[2]:15.8f} {specie:>2}\n'
                    f.write(s)
                    _string_cache[key] = s
                if self.constrains:
                    for con in self.constrains[i]:
                        f.write(f'constrain_relaxation {con}\n')
        elif fmt == 'mopac':
            f.write('* Formula: {}\n'.format(self.formula))
            for specie, coord in self:
                f.write('{:>2} {}\n'.format(
                    specie, ' '.join('{:15.8} 1'.format(x) for x in coord)
                ))
        else:
            raise ValueError("Unknown format: '{}'".format(fmt))

    def copy(self) -> 'Molecule':
        return Molecule(self.species.copy(), self.coords.copy())

    def write(self, filename: str) -> None:
        ext = os.path.splitext(filename)[1]
        if ext == 'xyz':
            fmt = 'xyz'
        elif ext == 'aims' or os.path.basename(filename) == 'geometry.in':
            fmt = 'aims'
        elif ext == 'mopac':
            fmt = 'mopac'
        with open(filename, 'w') as f:
            self.dump(f, fmt)


class Crystal(Molecule):
    def __init__(self, species: List[str], coords: np.ndarray,
                 lattice: np.ndarray, **kwargs: Any) -> None:
        self.lattice = np.array(lattice)
        Molecule.__init__(self, species, coords, **kwargs)

    def dump(self, f: IO[str], fmt: str) -> None:
        if fmt == '':
            f.write(repr(self))
        elif fmt == 'aims':
            for label, (x, y, z) in zip('abc', self.lattice):
                f.write(f'lattice_vector {x:15.8f} {y:15.8f} {z:15.8f}\n')
                for con in self.constrains[label]:
                    f.write(f'constrain_relaxation {con}\n')
            super().dump(f, fmt)
        else:
            raise ValueError(f'Unknown format: {fmt!r}')

    def copy(self) -> 'Crystal':
        return Crystal(self.species.copy(), self.coords.copy(), self.lattice.copy())

    def get_kgrid(self, density: float = 0.06) -> Tuple[int, int, int]:
        rec_lattice = 2*np.pi*np.linalg.inv(self.lattice.T)
        rec_lens = np.sqrt((rec_lattice**2).sum(1))
        nkpts = np.ceil(rec_lens/(density*bohr))
        return int(nkpts[0]), int(nkpts[1]), int(nkpts[2])

    def supercell(self, ns: Tuple[int, int, int]) -> 'Crystal':
        latt_vectors = np.array([
            sum(s*vec for s, vec in zip(shift, self.lattice))
            for shift in product(*map(range, ns))
        ])
        species = list(chain.from_iterable(repeat(self.species, len(latt_vectors))))
        coords = [
            tuple(vec) for vec in
            (self.xyz[None, :, :]+latt_vectors[:, None, :]).reshape((-1, 3))
        ]
        lattice = self.lattice*np.array(ns)[:, None]
        if self.constrains:
            constrains = DefaultDict[Union[str, int], List[str]](list)
            for i in range(len(species)):
                i_orig = i % len(self)
                try:
                    constrains[i] = self.constrains[i_orig]
                except KeyError:
                    pass
            for label in 'abc':
                constrains[label] = self.constrains[label]
        else:
            constrains = defaultdict(list)
        return Crystal(species, coords, lattice, constrains=constrains)


def get_vec(ws: List[str]) -> Vec:
    return float(ws[0]), float(ws[1]), float(ws[2])


def load(fp: IO[str], fmt: str) -> Molecule:
    if fmt == 'xyz':
        n = int(fp.readline())
        fp.readline()
        species = []
        coords = []
        for _ in range(n):
            ws = fp.readline().split()
            species.append(ws[0])
            coords.append(get_vec(ws[1:4]))
        return Molecule(species, coords)
    if fmt == 'aims':
        species = []
        coords = []
        lattice = []
        while True:
            l = fp.readline()
            if l == '':
                break
            l = l.strip()
            if not l or l.startswith('#'):
                continue
            ws = l.split()
            what = ws[0]
            if what == 'atom':
                species.append(ws[4])
                coords.append(get_vec(ws[1:4]))
            elif what == 'lattice_vector':
                lattice.append(get_vec(ws[1:4]))
        if lattice:
            assert len(lattice) == 3
            return Crystal(species, coords, lattice)
        else:
            return Molecule(species, coords)
    raise ValueError(f'Unknown format: {fmt}')


def loads(s: str, fmt: str) -> Molecule:
    fp = StringIO(s)
    return load(fp, fmt)


def readfile(path: str, fmt: str = None) -> Molecule:
    if not fmt:
        ext = os.path.splitext(path)[1]
        if ext == '.xyz':
            fmt = 'xyz'
        elif ext == '.aims' or os.path.basename(path) == 'geometry.in':
            fmt = 'aims'
        else:
            raise RuntimeError('Cannot determine format')
    with open(path) as f:
        return load(f, fmt)
