from metaflow.api import Flow, step


class Flow1(metaclass=Flow):
    @step
    def one(self):
        self.a = 111
