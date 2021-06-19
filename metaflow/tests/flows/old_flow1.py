from metaflow import FlowSpec, step


class OldFlow1(FlowSpec):
    @step
    def start(self):
        self.next(self.one)

    @step
    def one(self):
        self.a = 111
        self.next(self.end)

    @step
    def end(self):
        pass
