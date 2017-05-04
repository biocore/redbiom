from importlib import import_module

import click

from redbiom import __version__


def _terribly_handle_brokenpipeerror():
    # based off http://stackoverflow.com/a/34299346
    import os
    import sys
    sys.stdout = os.fdopen(1, 'w')


@click.group()
@click.version_option(version=__version__)
@click.pass_context
def cli(ctx):
    ctx.call_on_close(_terribly_handle_brokenpipeerror)


import_module('redbiom.commands.admin')
import_module('redbiom.commands.search')
import_module('redbiom.commands.select')
import_module('redbiom.commands.summarize')
import_module('redbiom.commands.fetch')
