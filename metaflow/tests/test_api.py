"""Basic pytest of a simple FlowSpec."""

from metaflow.tests.flows import LinearFlow
from metaflow.tests.utils import check_graph, run, flow_path


def test_api():
    flow = LinearFlow

    # Verify class-level properties: `file`, `name`, `path_spec`
    file = flow_path("linear_flow.py")
    assert flow.file == file
    assert flow.name == "LinearFlow"
    assert flow.path_spec == ("%s:%s" % (file, flow.name))

    # Verify graph; note that @step-function line numbers point at the `@step` decorator in Python ≤3.7; in Python ≥3.8,
    # they point at the function `def` line. When Metaflow's CI adds support for newer Python versions, these expected
    # line numbers will have to take the Python version into account. See also: https://bugs.python.org/issue33211.
    # fmt: off
    expected = [
        {'name': 'start', 'type':  'start', 'in_funcs': [       ], 'out_funcs': [  'one'], 'func_lineno': 10, },
        {'name':   'one', 'type': 'linear', 'in_funcs': ['start'], 'out_funcs': [  'two'], 'func_lineno': 14, },
        {'name':   'two', 'type': 'linear', 'in_funcs': [  'one'], 'out_funcs': ['three'], 'func_lineno': 19, },
        {'name': 'three', 'type': 'linear', 'in_funcs': [  'two'], 'out_funcs': [  'end'], 'func_lineno': 24, },
        {'name':   'end', 'type':    'end', 'in_funcs': ['three'], 'out_funcs': [       ], 'func_lineno': 30, },
    ]
    # fmt: on

    # Verify graph
    check_graph(flow, expected)

    # Verify runs + data
    data = run(flow)

    # Verify fields set during flow execution
    assert data == {
        "a": 111,
        "b": 222,
        "checked": True,
    }
