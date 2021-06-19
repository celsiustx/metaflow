from metaflow.api import Flow, step


class Flow3(metaclass=Flow):
    @step
    def three(self):
        assert (self.a, self.b) == (111, 222)
        self.checked = True
