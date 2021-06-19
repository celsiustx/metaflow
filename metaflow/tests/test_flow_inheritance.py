from os.path import join
from pytest import raises

from metaflow.tests.flows import Flow12, Flow123
from metaflow.tests.utils import check_graph, metaflow_bin, metaflow_version, test_flows_dir, run, verify_output
from metaflow.util import resolve_identity


def flow_path(name):
    return join(test_flows_dir, name)


flow12_graph = [
    {'name': 'start', 'type': 'linear', 'in_funcs': [         ], 'out_funcs': [ 'one' ], 'split_parents': [], 'file': flow_path('inherited_flows.py'), 'func_lineno':  7, },
    {'name':   'one', 'type': 'linear', 'in_funcs': [ 'start' ], 'out_funcs': [ 'two' ], 'split_parents': [], 'file': flow_path(          'flow1.py'), 'func_lineno':  4, },
    {'name':   'two', 'type': 'linear', 'in_funcs': [   'one' ], 'out_funcs': [ 'end' ], 'split_parents': [], 'file': flow_path(          'flow2.py'), 'func_lineno':  4, },
    {'name':   'end', 'type':    'end', 'in_funcs': [   'two' ], 'out_funcs': [       ], 'split_parents': [], 'file': flow_path('inherited_flows.py'), 'func_lineno': 10, },
]

def test_flow12():
    check_graph(Flow12, flow12_graph)

    # Verify Flow
    data = run(Flow12)
    assert (data.a, data.b) == (111, 222)
    # `checked` is set in step `three` from Flow3, which isn't mixed into Flow12
    with raises(KeyError):
        print(data.checked)


flow123_graph = [
    {'name': 'start', 'type': 'linear', 'in_funcs': [         ], 'out_funcs': [   'one' ], 'split_parents': [], 'file': flow_path('inherited_flows.py'), 'func_lineno': 11, },
    {'name':   'one', 'type': 'linear', 'in_funcs': [ 'start' ], 'out_funcs': [   'two' ], 'split_parents': [], 'file': flow_path(          'flow1.py'), 'func_lineno':  4, },
    {'name':   'two', 'type': 'linear', 'in_funcs': [   'one' ], 'out_funcs': [ 'three' ], 'split_parents': [], 'file': flow_path(          'flow2.py'), 'func_lineno':  4, },
    {'name': 'three', 'type': 'linear', 'in_funcs': [   'two' ], 'out_funcs': [   'end' ], 'split_parents': [], 'file': flow_path(          'flow3.py'), 'func_lineno':  4, },
    {'name':   'end', 'type':    'end', 'in_funcs': [ 'three' ], 'out_funcs': [         ], 'split_parents': [], 'file': flow_path('inherited_flows.py'), 'func_lineno': 14, },
]


def test_flow123():
    # Verify Flow
    data = run(Flow123)
    assert (data.a, data.b, data.checked) == (111, 222, True)
    with raises(KeyError):
        print(data.c)


def test_flow123_show():
    user = resolve_identity()
    cmd = [metaflow_bin,'flow',Flow123.path_spec,'show']
    stdout = """
Step one
    ?
    => two

Step two
    ?
    => three

Step three
    ?
    => end

Step start
    ?
    => one

Step end
    ?
"""
    stderr = 'Metaflow {version} executing {flow} for {user}\n\n\n\n'.format(
        version=metaflow_version, flow='Flow123', user=user,
    )
    verify_output(cmd, stdout, stderr)


def test_broken_inheritance():
    with raises(RuntimeError, match='Flow A: refusing to mix in multiple implementations of step "a"'):
        from metaflow.tests.flows.broken_inheritance import A
