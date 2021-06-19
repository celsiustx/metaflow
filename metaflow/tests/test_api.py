"""Basic pytest of a simple "current API" flow."""

from pytest import raises

from metaflow import FlowSpec, step
from metaflow.tests.utils import check_graph, parametrize, run


class OldFlow(FlowSpec):
    step = mf.step

    @step
    def start(self):
        self.next(self.one)

    @step
    def one(self):
        self.a = 111
        self.next(self.two)

    @step
    def two(self):
        self.b = self.a * 2
        self.next(self.three)

    @step
    def three(self):
        assert (self.a, self.b, self.foo, self.mth()) == (111, 222, '`foo`', '`mth`')
        self.checked = True
        self.next(self.end)

    @step
    def end(self):
        pass

    @property
    def foo(self):
        return '`foo`'

    def mth(self):
        return '`mth`'


@parametrize('flow', [ OldFlow, ])
def test_api(flow):
    # Verify graph
    expected = [
        {'name': 'start', 'type': 'linear', 'in_funcs': [       ], 'out_funcs': [  'one'], },
        {'name':   'one', 'type': 'linear', 'in_funcs': ['start'], 'out_funcs': [  'two'], },
        {'name':   'two', 'type': 'linear', 'in_funcs': [  'one'], 'out_funcs': ['three'], },
        {'name': 'three', 'type': 'linear', 'in_funcs': [  'two'], 'out_funcs': [  'end'], },
        {'name':   'end', 'type':    'end', 'in_funcs': ['three'], 'out_funcs': [       ], },
    ]

    check_graph(flow, expected)

    # Verify runs + data
    data = run(flow)

    # Verify fields set during flow execution
    assert (data.a, data.b, data.checked) == (111, 222, True)

    # Verify unset fields raise
    with raises(KeyError):
        print(data.c)
