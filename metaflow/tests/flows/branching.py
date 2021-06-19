
import metaflow as mf
from metaflow import FlowSpec
from metaflow.api import Flow, step, join


class OldBranchingFlow(FlowSpec):
    step = mf.step

    @step
    def start(self):
        self.next(self.one)

    @step
    def one(self):
        self.n = 11
        self.next(self.aaa, self.bbb)

    @step
    def aaa(self):
        self._aaa = 'A' * self.n
        self.next(self.join)

    @step
    def bbb(self):
        self._bbb = 'B' * self.n
        self.next(self.join)

    @step
    def join(self, inputs):
        assert (inputs.aaa._aaa, inputs.bbb._bbb) == ('AAAAAAAAAAA','BBBBBBBBBBB')
        self.a, self.b = (inputs.aaa._aaa, inputs.bbb._bbb)
        assert not hasattr(self, 'n')
        [self.n] = {inputs.aaa.n, inputs.bbb.n}
        self.done = True
        self.next(self.end)

    @step
    def end(self): pass


class NewBranchingFlow(mf.FlowSpec, metaclass=Flow):
    @step
    def one(self):
        self.n = 11

    @step('one')
    def aaa(self):
        self._aaa = 'A' * self.n

    @step('one')
    def bbb(self):
        self._bbb = 'B' * self.n

    @join('aaa','bbb')
    def join(self, inputs):
        assert (inputs.aaa._aaa, inputs.bbb._bbb) == ('AAAAAAAAAAA','BBBBBBBBBBB')
        self.a, self.b = (inputs.aaa._aaa, inputs.bbb._bbb)
        assert not hasattr(self, 'n')
        [self.n] = {inputs.aaa.n, inputs.bbb.n}
        self.done = True
