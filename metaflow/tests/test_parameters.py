from click.exceptions import NoSuchOption
from pytest import raises

from metaflow import FlowSpec, Parameter
from metaflow.api import Flow, step
from metaflow.tests.utils import metaflow_bin, parametrize, run


class ParameterFlow1(FlowSpec, metaclass=Flow):
    debug = Parameter("debug", required=False, type=bool, default=None)
    @step
    def run(self):
        if self.debug is True:
            self.msg = 'debug mode'
        elif self.debug is False:
            self.msg = 'regular mode'
        else:
            assert self.debug is None
            self.msg = 'default mode'


@parametrize("sdk", [
    "python",
    "shell",
])
@parametrize("debug,msg", [
    [ None, 'default mode'],
    [False, 'regular mode'],
    [ True,   'debug mode'],
])
def test_parameter_flow1(sdk, debug, msg):
    if debug is None:
        args = []
    else:
        args = ['--debug', str(debug)]

    if sdk == 'python':
        data = run(ParameterFlow1, args=['run',] + args)
    else:
        data = run('ParameterFlow1', cmd=[ metaflow_bin, 'flow', ParameterFlow1.path_spec, 'run', ] + args)

    assert data.msg == msg


class ParameterFlow2(FlowSpec, metaclass=Flow):
    string = Parameter("str", required=False, type=str, default='default')
    @step
    def run(self):
        self.upper = self.string.upper()


class ParameterFlow3(FlowSpec, metaclass=Flow):
    int = Parameter("int", required=True, type=int, default=1)
    @step
    def run(self):
        self.squared = self.int * self.int


def test_clear_main_flow():
    # Normal ParameterFlow2 run with "--str" flag
    data = run(ParameterFlow2, args=['run','--str','bbb'])
    assert data.upper == 'BBB'

    # ParameterFlow3's "--int" flag is not allowed
    with raises(NoSuchOption):
        run(ParameterFlow2, args=['run','--int','111'])

    # ParameterFlow2/"--str" still works
    data = run(ParameterFlow2, args=['run','--str','cccc'])
    assert data.upper == 'CCCC'

    # Switch to ParameterFlow3 / "--int" flag
    data = run(ParameterFlow3, args=['run','--int','11'])
    assert data.squared == 121

    # ParameterFlow2's "--str" flag is not allowed
    with raises(NoSuchOption):
        run(ParameterFlow3, args=['run','--str','ddd'])

    # ParameterFlow3/"--int" still works
    data = run(ParameterFlow3, args=['run','--int','100'])
    assert data.squared == 10000
