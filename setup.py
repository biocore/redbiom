#!/usr/bin/env python

# ----------------------------------------------------------------------------
# Copyright (c) 2016--, redbiom development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
# ----------------------------------------------------------------------------

from setuptools import setup, find_packages

# adapted from q2cli's setup.py

setup(
    name='redbiom',
    version='2017.0.0.dev0',
    license='BSD-3-Clause',
    url='https://github.com/wasade/biocore',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['click >= 6.7', 'biom-format >= 2.1.5', 'requests',
                      'pandas'],
    #scripts=['bin/tab-redbiom'],
    entry_points='''
        [console_scripts]
        redbiom=redbiom.commands:cli
    ''',
)
