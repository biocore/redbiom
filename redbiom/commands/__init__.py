from importlib import import_module

import click

from redbiom import __version__


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


import_module('redbiom.commands.admin')
import_module('redbiom.commands.search')
import_module('redbiom.commands.summarize')
import_module('redbiom.commands.fetch')
