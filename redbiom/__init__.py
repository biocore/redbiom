# ----------------------------------------------------------------------------
# Copyright (c) 2017, The redbiom Development Team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
# ----------------------------------------------------------------------------

from __future__ import division

import atexit


# adapted from biom-format

__version__ = '2017.0.1.dev0'

active_sessions = []


def _close_sessions():
    # be polite
    for s in active_sessions:
        s.close()


atexit.register(_close_sessions)


def get_config():
    """Deal with all the configy bits"""
    import os
    import requests.auth
    user = os.environ.get('SEQUENCE_SEARCH_USER')
    password = os.environ.get('SEQUENCE_SEARCH_PASSWORD')
    hostname = os.environ.get('SEQUENCE_SEARCH_HOST', 'http://127.0.0.1:7379')

    if user is None:
        auth = None
    else:
        auth = requests.auth(user, password)
    return {'auth': auth, 'hostname': hostname}
