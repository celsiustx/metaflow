"""Test concordance between a "new API" flow (`NewFlow`) and an equivalent "current API" flow (`OldFlow`), both defined
in this file.
"""

from pytest import raises

import metaflow as mf
from metaflow import FlowSpec
from metaflow.api import Flow, step
from metaflow.tests.utils import check_graph, parametrize, run


# Note that the explicit `FlowSpec` inheritance here is optional (it will be added by the `Flow` metaclass if omitted).
# Including it helps IntelliJ to analyze/syntax-highlight member accesses downstream.
#
# Some functionality, like referencing `self.input` in a join step, gets flagged by Pylint if the FlowSpec-inheritance
# isn't made explicit.
#
# TODO: get Pylint to accept self.input references in Flows w/o FlowSpec explicitly specified
class NewFlow(FlowSpec, metaclass=Flow):
    @step
    def one(self):
        self.a = 111

    @step
    def two(self):
        self.b = self.a * 2

    @step
    def three(self):
        assert (self.a, self.b, self.foo, self.mth()) == (111, 222, '`foo`', '`mth`')
        self.checked = True

    @property
    def foo(self):
        return '`foo`'

    def mth(self):
        return '`mth`'


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


@parametrize('flow', [ NewFlow, OldFlow, ])
def test_api(flow):
    # Verify NewFlow and OldFlow graphs match
    expected = [
        {'name': 'start', 'type': 'linear', 'in_funcs': [       ], 'out_funcs': [  'one'], },
        {'name':   'one', 'type': 'linear', 'in_funcs': ['start'], 'out_funcs': [  'two'], },
        {'name':   'two', 'type': 'linear', 'in_funcs': [  'one'], 'out_funcs': ['three'], },
        {'name': 'three', 'type': 'linear', 'in_funcs': [  'two'], 'out_funcs': [  'end'], },
        {'name':   'end', 'type':    'end', 'in_funcs': ['three'], 'out_funcs': [       ], },
    ]

    check_graph(flow, expected)

    # Verify NewFlow and OldFlow runs + data
    data = run(flow)

    # Verify fields set during flow execution
    assert (data.a, data.b, data.checked) == (111, 222, True)

    # Verify unset fields raise
    with raises(KeyError):
        print(data.c)
