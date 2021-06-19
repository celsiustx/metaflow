
from os.path import join
from pytest import raises
from subprocess import CalledProcessError, check_call
from sys import executable as python

import metaflow
import metaflow.metaflow_version
from metaflow.tests.utils import \
    metaflow_bin, metaflow_dir, metaflow_version, \
    parametrize, run, tests_dir, test_flows_dir, verify_output
from metaflow.util import resolve_identity


test_api_path = join(tests_dir, 'test_api.py')
old_flow123_path = join(test_flows_dir, 'old_flow123.py')
old_flow1_path = join(test_flows_dir, 'old_flow1.py')


flow_files = {
    'OldFlow': test_api_path,
    'OldFlow1': old_flow1_path,
    'OldFlow12': old_flow123_path,
    'OldFlow123': old_flow123_path,
}

# Verify running flows without a subcommand prints the expected subcommand-listing / help msg
@parametrize('flow', ['OldFlow',])
def test_flow_base_cmd(flow):
    file = flow_files[flow]
    flow_path_spec = '%s:%s' % (file, flow)
    user = resolve_identity()
    cmd = [ metaflow_bin, 'flow', flow_path_spec ]
    expected_stdout = ''
    expected_stderr = '''Metaflow {version} executing {flow} for {user}
Validating your flow...
    The graph looks good!
Running pylint...
    Pylint is happy!

'metaflow flow {file}:{flow} show' shows a description of this flow.
'metaflow flow {file}:{flow} run' runs the flow locally.
'metaflow flow {file}:{flow} help' shows all available commands and options.

'''.format(file=file, flow=flow, user=user, version=metaflow_version)
    verify_output(cmd, expected_stdout, expected_stderr)


cases = {
    'python': dict(
        entrypoint=[python,'-m','metaflow.main_cli'],
        # `cmd_str` is a piece of stdout from `metaflow flow <file>:<flow> show` that we verify
        cmd_str="'{python}', '{d}/main_cli.py'".format(python=python, d=metaflow_dir),
    ),
    'metaflow': dict(
        entrypoint=[metaflow_bin],
        cmd_str="'{metaflow}'".format(metaflow=metaflow_bin),
    ),
}
@parametrize('case', cases.keys())
@parametrize('flow', ['OldFlow',])
def test_show(case, flow):
    '''Test `show`ing `OldFlow` via a given CLI entrypoint'''
    case = cases[case]
    file = flow_files[flow]
    entrypoint = case['entrypoint']
    user = resolve_identity()
    expected_err = 'Metaflow {v} executing {flow} for {user}\n\n\n\n'.format(v=metaflow_version, flow=flow, user=user)
    expected_out = '''
Step start
    ?
    => one

Step one
    ?
    => two

Step two
    ?
    => three

Step three
    ?
    => end

Step end
    ?
'''
    flow_path_spec = '%s:%s' % (file, flow)
    cmd = entrypoint + ['flow',flow_path_spec,'show']
    verify_output(cmd, expected_out, expected_err)


@parametrize('flow', ['OldFlow',])
@parametrize('entrypoint', [[python,'-m','metaflow.main_cli'],[metaflow_bin]])
def test_run(flow, entrypoint):
    # TODO: use a fresh metaflow db in a tempdir to avoid races / concurrent runs by different processes
    flow_path_spec = '%s:%s' % (flow_files[flow], flow)
    cmd = entrypoint + [ 'flow', flow_path_spec, 'run']
    data = run(flow, cmd=cmd)
    assert (data.a, data.b, data.checked) == (111, 222, True)


# Test [a flow from a file with a '__main__' that calls that flow] in 3 ways:
# - via that __main__: `python <file>`
# - explicitly via a flow-path spec (disregarding the '__main__' handler): `python -m metaflow.main_cli flow <file>:<flow> run`
# - via the `metaflow` CLI: `metaflow flow <file>:<flow> run`
cmds = {
    'python module': [ python,'-m','metaflow.main_cli','flow', '{flow_path_spec}', 'run', ],
    'metaflow': [ metaflow_bin, 'flow', '{flow_path_spec}', 'run', ],
    'python file': [ python, '{flow_path}', 'run', ],
}
@parametrize('name', cmds.keys())
@parametrize('flow', ['OldFlow123',])
def test_run_via_main(name, flow):
    file = flow_files[flow]
    cmd = cmds[name]
    flow_path_spec = '%s:%s' % (file, flow)
    cmd = [ arg.format(flow_path_spec=flow_path_spec, flow_path=file) for arg in cmd ]
    data = run(flow, cmd=cmd)
    assert (data.a, data.b, data.checked) == (111, 222, True)


@parametrize('name', ['python module', 'metaflow'])
@parametrize('flow', ['OldFlow12',])
def test_run_nondefault_despite_main(name, flow):
    cmd = cmds[name]
    file = flow_files[flow]
    flow_path_spec = '%s:%s' % (file, flow)
    cmd = [ arg.format(flow_path_spec=flow_path_spec, flow_path=file) for arg in cmd ]
    data = run(flow, cmd=cmd)
    assert (data.a, data.b) == (111, 222)
    with raises(KeyError):
        data.checked


@parametrize('flow', ['OldFlow1',])
def test_flow_file_form(flow):
    file = flow_files[flow]
    user = resolve_identity()
    cmd = [metaflow_bin,'flow',file,'show']
    stdout = '''
Step start
    ?
    => one

Step one
    ?
    => end

Step end
    ?
'''
    stderr = 'Metaflow {version} executing {flow} for {user}\n\n\n\n'.format(
        version=metaflow_version, flow=flow, user=user,
    )
    verify_output(cmd, stdout, stderr)

    with raises(CalledProcessError):
        check_call([metaflow_bin,'flow',inherited_flows_path,'show'])
