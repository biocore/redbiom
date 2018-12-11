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

__version__ = '0.3.0'

# db version follows macro/minor/micro expectations where a micro change should
# be backwards compatible, a minor change introduces some backwards
# incompatibility, and a major change represents a large shift in the
# representation
__db_version__ = '0.3.0'

active_sessions = {}


def _close_sessions():
    # be polite
    for _, session in active_sessions.items():
        session.close()


atexit.register(_close_sessions)


def get_config():
    """Deal with all the configy bits"""
    import os
    hostname = os.environ.get('REDBIOM_HOST', 'http://qiita.ucsd.edu:7329')

    return {'hostname': hostname}
