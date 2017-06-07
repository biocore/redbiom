#!/usr/bin/env python

# ----------------------------------------------------------------------------
# Copyright (c) 2016--, redbiom development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
# ----------------------------------------------------------------------------

from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop

# based on http://stackoverflow.com/a/36902139

def _post():
    import nltk
    nltk.download('stopwords')
    nltk.download('punkt')

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        _post()

class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        develop.run(self)
        _post()


# adapted from q2cli's setup.py

setup(
    name='redbiom',
    version='2017.0.0.dev0',
    license='BSD-3-Clause',
    url='https://github.com/wasade/biocore',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['click >= 6.7', 'biom-format >= 2.1.5', 'requests',
                      'h5py', 'numpy', 'scipy',
                      'pandas', 'nltk', 'joblib', 'scikit-bio >= 0.4.2'],
    #scripts=['bin/tab-redbiom'],
    entry_points='''
        [console_scripts]
        redbiom=redbiom.commands:cli
    ''',
    cmdclass={'install': PostInstallCommand,
              'develop': PostDevelopCommand}
)
