from metaflow.api import Flow
from metaflow.tests.flows import Flow1
from metaflow.tests.flows import Flow2
from metaflow.tests.flows import Flow3


class Flow12(Flow1, Flow2, metaclass=Flow):
    pass


class Flow123(Flow1, Flow2, Flow3, metaclass=Flow):
    pass


if __name__ == '__main__':
    # Optional: set a default flow / enable running via the current `python <file>` CLI form
    Flow123()
