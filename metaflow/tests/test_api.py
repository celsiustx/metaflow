"""Test concordance between a simple "new API" flow (`NewLinearFlow`) and equivalent "current API" flow (`LinearFlow`).
"""

from metaflow.tests.flows import LinearFlow, NewLinearFlow
from metaflow.tests.utils import check_graph, flow_path, parametrize, run


@parametrize(
    "flow,file,name",
    [
        (
            LinearFlow,
            "linear_flow.py",
            "LinearFlow",
        ),
        (
            NewLinearFlow,
            "new_linear_flow.py",
            "NewLinearFlow",
        ),
    ],
)
def test_flowspec_attrs(flow, file, name):
    # Verify class-level properties: `file`, `name`, `path_spec`
    file = flow_path(file)
    assert flow.file == file
    assert flow.name == name
    assert flow.path_spec == ("%s:%s" % (file, name))


line_no_map = {
    LinearFlow: [
        9,
        13,
        18,
        23,
        29,
    ],
    NewLinearFlow: [
        10,
        11,
        15,
        19,
        31,
    ],
}


@parametrize(
    "flow,file",
    [
        (
            LinearFlow,
            "linear_flow.py",
        ),
        (
            NewLinearFlow,
            "new_linear_flow.py",
        ),
    ],
)
def test_api(flow, file):
    # Add `@step` line numbers (different for each flow) to expected output
    expected = [
        {
            "name": "start",
            "type": "linear",
            "in_funcs": [],
            "out_funcs": ["one"],
        },
        {
            "name": "one",
            "type": "linear",
            "in_funcs": ["start"],
            "out_funcs": ["two"],
        },
        {
            "name": "two",
            "type": "linear",
            "in_funcs": ["one"],
            "out_funcs": ["three"],
        },
        {
            "name": "three",
            "type": "linear",
            "in_funcs": ["two"],
            "out_funcs": ["end"],
        },
        {
            "name": "end",
            "type": "end",
            "in_funcs": ["three"],
            "out_funcs": [],
        },
    ]
    line_nos = line_no_map[flow]
    expected = [
        {
            **o,
            "func_lineno": lineno,
        }
        for o, lineno in zip(expected, line_nos)
    ]

    # Verify old and new flow graphs match; note that @step-function line numbers point at the `@step` decorator in
    # Python ≤3.7; in Python ≥3.8, they point at the function `def` line. When Metaflow's CI adds support for newer
    # Python versions, these expected line numbers will have to take the Python version into account. See also:
    # https://bugs.python.org/issue33211.
    check_graph(flow, expected)

    # Verify that the flow runs successfully and that data artifacts are as expected.
    data = run(flow)

    # Verify fields set during flow execution
    assert data == {
        "a": 111,
        "b": 222,
        "checked": True,
    }
