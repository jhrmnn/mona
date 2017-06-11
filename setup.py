from setuptools import setup, find_packages


setup(
    name='caf',
    version='0.3.0',
    description='Distributed calculation framework',
    url='https://github.com/azag0/caf',
    author='Jan Hermann',
    author_email='dev@hermann.in',
    license='MIT',
    packages=['caflib', 'caflib.extras', 'caflib.Tools'],
    scripts=['caf'],
    classifieres=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.6',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Utilities',
    ],
    install_requires=['docopt'],
)
