#!/usr/bin/env python

# ----------------------------------------------------------------------------
# Copyright (c) 2016--, redbiom development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
# ----------------------------------------------------------------------------

from setuptools import setup, find_packages


long_description = open('README.md').read()


# adapted from q2cli's setup.py

setup(
    name='redbiom',
    version='0.3.5',
    license='BSD-3-Clause',
    author='Daniel McDonald',
    author_email='wasade@gmail.com',
    url='https://github.com/biocore/redbiom',
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type='text/markdown',
    include_package_data=True,
    install_requires=['click >= 6.7', 'biom-format >= 2.1.5',
                      'requests', 'h5py', 'pandas', 'nltk',
                      'joblib', 'scikit-bio >= 0.4.2', 'msgpack'],
    entry_points='''
        [console_scripts]
        redbiom=redbiom.commands:cli
    '''
)
