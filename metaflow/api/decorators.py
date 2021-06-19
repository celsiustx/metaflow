
from metaflow.meta import FOREACH, IFF, IFN, IS_STEP, JOIN, META_KEY, PREV_STEP


def get_meta(f, default=None):
    if hasattr(f, META_KEY):
        return getattr(f, META_KEY)
    else:
        meta = default or {}
        setattr(f, META_KEY, meta)
        return meta


def step(arg):
    '''Alternate "step" decorator; activates graph-parsing flow that injects start/end steps and self.next() calls.'''
    def make_step(f, prev):
        meta = get_meta(f)
        meta[IS_STEP] = True
        setattr(f, IS_STEP, True)
        f.decorators = []
        meta[PREV_STEP] = prev
        return f
    if isinstance(arg, str):
        return lambda f: make_step(f, arg)
    elif callable(arg):
        return make_step(arg, None)
    else:
        raise ValueError('@step must be passed a string step name or called directly on a method')


def foreach(field, step=None):
    def make_foreach(f, step, field):
        meta = get_meta(f)
        meta[FOREACH] = dict(step=step, field=field)
        meta[IS_STEP] = True
        setattr(f, IS_STEP, True)
        f.decorators = []
        return f
    if not isinstance(field, str):
        raise ValueError('`field` param to `@foreach` must be a string name of an instance var')
    return lambda f: make_foreach(f, step, field)


def join(*args):
    def make_join(f, args):
        meta = get_meta(f)
        meta[JOIN] = args
        meta[IS_STEP] = True
        setattr(f, IS_STEP, True)
        f.decorators = []
        return f
    if args and all(isinstance(arg, str) for arg in args):
        '''Support usage like `@join('key')`'''
        return lambda f: make_join(f, args)
    else:
        # Support direct, no-arg `@join` decoration (applies to the last step)
        if len(args) == 1:
            arg = args[0]
            assert callable(arg)
            return make_join(arg, None)
        else:
            raise ValueError('@join must be called directly on a step function, or passed 1 or more step-names to join')


def iff(*args):
    if len(args) == 1:
        step, key = None, args[0]
    elif len(args) == 2:
        step, key = args
    else:
        raise ValueError('Expected 1 or 2 args to iff()')
    def make_iff(f):
        meta = get_meta(f)
        meta[IFF] = dict(step=step, key=key)
        meta[IS_STEP] = True
        setattr(f, IS_STEP, True)
        f.decorators = []
        return f
    return make_iff


def ifn(*args):
    if len(args) == 1:
        step, key = None, args[0]
    elif len(args) == 2:
        step, key = args
    else:
        raise ValueError('Expected 1 or 2 args to ifn()')
    def make_ifn(f):
        meta = get_meta(f)
        meta[IFN] = dict(step=step, key=key)
        meta[IS_STEP] = True
        setattr(f, IS_STEP, True)
        f.decorators = []
        return f
    return make_ifn
