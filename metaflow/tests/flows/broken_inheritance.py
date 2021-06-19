import metaflow as mf
from metaflow.api import Flow, step


class A1(mf.FlowSpec, metaclass=Flow):
    @step
    def a(self): pass


class A2(mf.FlowSpec, metaclass=Flow):
    @step
    def a(self): pass


class A(A1, A2, metaclass=Flow): pass
