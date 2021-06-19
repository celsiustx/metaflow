from metaflow.api import Flow, step


class Flow2(metaclass=Flow):
    @step
    def two(self):
        self.b = self.a * 2
