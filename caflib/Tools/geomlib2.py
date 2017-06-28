# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import chain, product, repeat
import os
from io import StringIO
import numpy as np  # type: ignore

from typing import (  # noqa
    List, Tuple, DefaultDict, Iterator, IO, Sized, Iterable, Union, Dict, Any,
    Optional
)

from . import geomlib


specie_data = geomlib.Atom.data
bohr = geomlib.bohr


Vec = Tuple[float, float, float]


_string_cache: Dict[Any, str] = {}


class Atom:
    def __init__(self, specie: str, coord: Vec, ghost: bool = False) -> None:
        self.specie = specie
        self.coord = coord
        self.ghost = ghost

    @property
    def number(self) -> int:
        return int(specie_data[self.specie]['number'])

    def copy(self) -> 'Atom':
        return Atom(self.specie, self.coord, self.ghost)


class Molecule(Sized, Iterable):
    def __init__(self, atoms: List[Atom]) -> None:
        self._atoms = atoms

    @classmethod
    def from_coords(cls, species: List[str], coords: List[Vec]) -> 'Molecule':
        return cls([Atom(sp, coord) for sp, coord in zip(species, coords)])

    @property
    def species(self) -> List[str]:
        return [atom.specie for atom in self]

    @property
    def numbers(self) -> List[int]:
        return [atom.number for atom in self]

    @property
    def coords(self) -> List[Vec]:
        return [atom.coord for atom in self]

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

    @property
    def centers(self) -> Iterator[Atom]:
        yield from self._atoms

    def __iter__(self) -> Iterator[Atom]:
        yield from (atom for atom in self._atoms if not atom.ghost)

    def __len__(self) -> int:
        return len([atom for atom in self._atoms if not atom.ghost])

    def __format__(self, fmt: str) -> str:
        fp = StringIO()
        self.dump(fp, fmt)
        return fp.getvalue()

    def items(self) -> Iterator[Tuple[str, Vec]]:
        for atom in self:
            yield atom.specie, atom.coord

    dumps = __format__

    def dump(self, f: IO[str], fmt: str) -> None:
        if fmt == '':
            f.write(repr(self))
        elif fmt == 'xyz':
            f.write('{}\n'.format(len(self)))
            f.write('Formula: {}\n'.format(self.formula))
            for specie, coord in self.items():
                f.write('{:>2} {}\n'.format(
                    specie, ' '.join('{:15.8}'.format(x) for x in coord)
                ))
        elif fmt == 'aims':
            for atom in self.centers:
                specie, r = atom.specie, atom.coord
                key = (specie, r, atom.ghost, fmt)
                try:
                    f.write(_string_cache[key])
                except KeyError:
                    kind = 'atom' if not atom.ghost else 'empty'
                    s = f'{kind} {r[0]:15.8f} {r[1]:15.8f} {r[2]:15.8f} {specie:>2}\n'
                    f.write(s)
                    _string_cache[key] = s
        elif fmt == 'mopac':
            f.write('* Formula: {}\n'.format(self.formula))
            for specie, coord in self.items():
                f.write('{:>2} {}\n'.format(
                    specie, ' '.join('{:15.8} 1'.format(x) for x in coord)
                ))
        else:
            raise ValueError("Unknown format: '{}'".format(fmt))

    def copy(self) -> 'Molecule':
        return Molecule([atom.copy() for atom in self._atoms])

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
    def __init__(self, atoms: List[Atom], lattice: List[Vec]) -> None:
        Molecule.__init__(self, atoms)
        self.lattice = lattice

    @classmethod
    def from_coords(cls, species: List[str], coords: List[Vec],  # type: ignore
                    lattice: List[Vec]) -> 'Crystal':
        return cls(
            [Atom(sp, coord) for sp, coord in zip(species, coords)],
            lattice
        )

    def dump(self, f: IO[str], fmt: str) -> None:
        if fmt == '':
            f.write(repr(self))
        elif fmt == 'aims':
            for label, (x, y, z) in zip('abc', self.lattice):
                f.write(f'lattice_vector {x:15.8f} {y:15.8f} {z:15.8f}\n')
            super().dump(f, fmt)
        else:
            raise ValueError(f'Unknown format: {fmt!r}')

    def copy(self) -> 'Crystal':
        return Crystal(
            [atom.copy() for atom in self._atoms],
            self.lattice.copy()
        )

    @property
    def abc(self) -> np.ndarray:
        return np.array(self.lattice)

    def get_kgrid(self, density: float = 0.06) -> Tuple[int, int, int]:
        rec_lattice = 2*np.pi*np.linalg.inv(self.abc.T)
        rec_lens = np.sqrt((rec_lattice**2).sum(1))
        nkpts = np.ceil(rec_lens/(density*bohr))
        return int(nkpts[0]), int(nkpts[1]), int(nkpts[2])

    def supercell(self, ns: Tuple[int, int, int]) -> 'Crystal':
        abc = self.abc
        latt_vectors = np.array([
            sum(s*vec for s, vec in zip(shift, abc))
            for shift in product(*map(range, ns))
        ])
        species = list(chain.from_iterable(repeat(self.species, len(latt_vectors))))
        coords = [
            (x, y, z) for x, y, z in
            (self.xyz[None, :, :]+latt_vectors[:, None, :]).reshape((-1, 3))
        ]
        lattice = [(x, y, z) for x, y, z in abc*np.array(ns)[:, None]]
        return Crystal.from_coords(species, coords, lattice)


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
        return Molecule.from_coords(species, coords)
    if fmt == 'aims':
        atoms = []
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
            if what in ['atom', 'empty']:
                atoms.append(Atom(ws[4], get_vec(ws[1:4]), ghost=what == 'empty'))
            elif what == 'lattice_vector':
                lattice.append(get_vec(ws[1:4]))
        if lattice:
            assert len(lattice) == 3
            return Crystal(atoms, lattice)
        else:
            return Molecule(atoms)
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
