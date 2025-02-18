import sys

import os

from metaflow import FlowSpec
from metaflow._vendor import click

from metaflow.extension_support.cmd import process_cmds, resolve_cmds
from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

from .util import echo_always


@click.group()
def main():
    pass


@main.command(help="Show all available commands.")
@click.pass_context
def help(ctx):
    print(ctx.parent.get_help())


@main.command(help="Show flows accessible from the current working tree.")
def status():
    from metaflow.client import get_metadata

    res = get_metadata()
    if res:
        res = res.split("@")
    else:
        raise click.ClickException("Unknown status: cannot find a Metadata provider")
    if res[0] == "service":
        echo("Using Metadata provider at: ", nl=False)
        echo('"%s"\n' % res[1], fg="cyan")
        echo("To list available flows, type:\n")
        echo("1. python")
        echo("2. from metaflow import Metaflow")
        echo("3. list(Metaflow())")
        return

    from metaflow.client import namespace, metadata, Metaflow

    # Get the local data store path
    path = LocalStorage.get_datastore_root_from_config(echo, create_on_absent=False)
    # Throw an exception
    if path is None:
        raise click.ClickException(
            "Could not find "
            + click.style('"%s"' % DATASTORE_LOCAL_DIR, fg="red")
            + " in the current working tree."
        )

    stripped_path = os.path.dirname(path)
    namespace(None)
    metadata("local@%s" % stripped_path)
    echo("Working tree found at: ", nl=False)
    echo('"%s"\n' % stripped_path, fg="cyan")
    echo("Available flows:", fg="cyan", bold=True)
    for flow in Metaflow():
        echo("* %s" % flow, fg="cyan")


@main.command(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
    help="Select a flow.",
)
@click.argument("flow_path")
@click.argument("extra_args", nargs=-1, type=click.UNPROCESSED)
def flow(flow_path, extra_args):
    flow_spec = FlowSpec.load(flow_path)
    flow_spec(args=extra_args)


CMDS_DESC = [("configure", ".configure_cmd.cli"), ("tutorials", ".tutorials_cmd.cli")]

process_cmds(globals())


@click.command(
    cls=click.CommandCollection,
    sources=[main] + resolve_cmds(),
    invoke_without_command=True,
)
@click.pass_context
def start(ctx):
    global echo
    echo = echo_always

    import metaflow

    if ctx.invoked_subcommand is None:
        echo(
            "Metaflow (%s): " % metaflow.__version__, fg="magenta", bold=False, nl=False
        )

    if ctx.invoked_subcommand is None:
        echo("More data science, less engineering\n", fg="magenta")

        # metaflow URL
        echo("http://docs.metaflow.org", fg="cyan", nl=False)
        echo(" - Read the documentation")

        # metaflow chat
        echo("http://chat.metaflow.org", fg="cyan", nl=False)
        echo(" - Chat with us")

        # metaflow help email
        echo("help@metaflow.org", fg="cyan", nl=False)
        echo("        - Get help by email\n")

        print(ctx.get_help())


if __name__ == "__main__":
    start()
