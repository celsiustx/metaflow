from metaflow import FlowSpec, step


class OldFlow12(FlowSpec):
    @step
    def start(self):
        self.a = 111
        self.next(self.end)
    @step
    def end(self):
        self.b = 222
        assert self.a == 111


class OldFlow123(FlowSpec):
    @step
    def start(self):
        self.a = 111
        self.next(self.end)
    @step
    def end(self):
        self.b = 222
        assert self.a == 111
        self.checked = True


if __name__ == '__main__':
    OldFlow123()
