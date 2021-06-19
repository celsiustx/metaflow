from collections import namedtuple
import json
from typing import Union

import click

from .exception import ParameterFieldFailed,\
                       ParameterFieldTypeMismatch,\
                       MetaflowException
from .util import get_username, is_stringish

try:
    # Python2
    strtype = basestring
except:
    # Python3
    strtype = str

# ParameterContext allows deploy-time functions modify their
# behavior based on the context. We can add fields here without
# breaking backwards compatibility but don't remove any fields!
ParameterContext = namedtuple('ParameterContext',
                              ['flow_name',
                               'user_name',
                               'parameter_name',
                               'logger',
                               'ds_type'])

# Global FlowSpec → List[Parameters] registry; initial value `None` indicates uninitialized state
_parameters: Union[None,dict] = None
context_proto = None


def register_parameters(flow):
    '''Register a flow class's Parameters with the global `_parameters` dict

    Called during FlowSpec.__init__ (if not during FlowSpec class construction)'''
    from .flowspec import FlowSpec
    if isinstance(flow, FlowSpec):
        cls = flow.__class__
    elif issubclass(flow, FlowSpec):
        cls = flow
    else:
        raise ValueError('Unrecognized flow/FlowSpec: %s' % str(flow))
    params = dict(flow._get_parameters())

    global _parameters
    if _parameters is None:
        _parameters = {}
    if cls not in _parameters:
        _parameters[cls] = params


# Record the main flow being executed in this global singleton, for applying the correct Parameter flags during
# metaflow.cli initialization
_flow = None

# Map from `click` commands to inserted Options corresponding to the main flow's Parameters
_param_opts: Union[None, dict] = None
def register_main_flow(flow, overwrite=False):
    '''Register a flow class as being the "main" executable flow for this Metaflow invocation

    Used for augmenting the Metaflow CLI with the correct flow's Parameter flags'''
    global _flow
    add_param_cmds = False
    if _flow is not None and _flow is not flow:
        if overwrite:
            clear_main_flow()
            add_param_cmds = True
        else:
            raise Exception(
                'metaflow.parameters._flow already registered (%s); refusing to overwrite with %s' % (
                    _flow.__name__, flow.__name__
                )
            )

    _flow = flow
    register_parameters(flow)

    if _param_opts and add_param_cmds:
        for cmd in _param_opts:
            add_custom_cmd_parameters(cmd)


def clear_main_flow(empty_ok=False):
    global _flow, _param_opts

    if _flow is None:
        if empty_ok:
            return
        else:
            raise RuntimeError("Main `_flow` singleton already empty, can't clear")

    assert _param_opts is not None
    _flow = None
    for cmd, v in _param_opts.items():
        param_opts = v['opts']
        while param_opts:
            param = param_opts.pop(0)
            cmd_param = cmd.params[0]
            if param is cmd_param:
                cmd.params.pop(0)
            else:
                raise RuntimeError(
                    'Attempting to remove parameter-option %s from cmd %s, found %s instead' % (
                        param.name,
                        cmd.name,
                        cmd_param.name,
                    )
                )


def has_main_flow():
    return _flow is not None


class JSONTypeClass(click.ParamType):
    name = 'JSON'

    def convert(self, value, param, ctx):
        try:
            return json.loads(value)
        except:
            self.fail("%s is not a valid JSON object" % value, param, ctx)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'JSON'

class DeployTimeField(object):
    """
    This a wrapper object for a user-defined function that is called
    at the deploy time to populate fields in a Parameter. The wrapper
    is needed to make Click show the actual value returned by the
    function instead of a function pointer in its help text. Also this
    object curries the context argument for the function, and pretty
    prints any exceptions that occur during evaluation.
    """
    def __init__(self,
                 parameter_name,
                 parameter_type,
                 field,
                 fun,
                 return_str=True,
                 print_representation=None):

        self.fun = fun
        self.field = field
        self.parameter_name = parameter_name
        self.parameter_type = parameter_type
        self.return_str = return_str
        self.print_representation = self.user_print_representation = print_representation
        if self.print_representation is None:
            self.print_representation = str(self.fun)

    def __call__(self, full_evaluation=False):
        # full_evaluation is True if there will be no further "convert" called
        # by click and the parameter should be fully evaluated.
        ctx = context_proto._replace(parameter_name=self.parameter_name)
        try:
            try:
                # Not all functions take two arguments
                val = self.fun(ctx, full_evaluation)
            except TypeError:
                val = self.fun(ctx)
        except:
            raise ParameterFieldFailed(self.parameter_name, self.field)
        else:
            return self._check_type(val)

    def _check_type(self, val):
        # it is easy to introduce a deploy-time function that that accidentally
        # returns a value whose type is not compatible with what is defined
        # in Parameter. Let's catch those mistakes early here, instead of
        # showing a cryptic stack trace later.

        # note: this doesn't work with long in Python2 or types defined as
        # click types, e.g. click.INT
        TYPES = {bool: 'bool',
                 int: 'int',
                 float: 'float',
                 list: 'list'}

        msg = "The value returned by the deploy-time function for "\
              "the parameter *%s* field *%s* has a wrong type. " %\
              (self.parameter_name, self.field)

        if self.parameter_type in TYPES:
            if type(val) != self.parameter_type:
                msg += 'Expected a %s.' % TYPES[self.parameter_type]
                raise ParameterFieldTypeMismatch(msg)
            return str(val) if self.return_str else val
        else:
            if not is_stringish(val):
                msg += 'Expected a string.'
                raise ParameterFieldTypeMismatch(msg)
            return val

    @property
    def description(self):
        return self.print_representation

    def __str__(self):
        if self.user_print_representation:
            return self.user_print_representation
        return self()

    def __repr__(self):
        if self.user_print_representation:
            return self.user_print_representation
        return self()


def deploy_time_eval(value):
    if isinstance(value, DeployTimeField):
        return value(full_evaluation=True)
    else:
        return value


# this is called by cli.main
def set_parameter_context(flow_name, echo, datastore):
    global context_proto
    context_proto = ParameterContext(flow_name=flow_name,
                                     user_name=get_username(),
                                     parameter_name=None,
                                     logger=echo,
                                     ds_type=datastore.TYPE)

class Parameter(object):
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs
        # TODO: check that the type is one of the supported types
        param_type = self.kwargs['type'] = self._get_type(kwargs)

        if self.name == 'params':
            raise MetaflowException("Parameter name 'params' is a reserved "
                                    "word. Please use a different "
                                    "name for your parameter.")

        # make sure the user is not trying to pass a function in one of the
        # fields that don't support function-values yet
        for field in ('show_default',
                      'separator',
                      'required'):
            if callable(kwargs.get(field)):
                raise MetaflowException("Parameter *%s*: Field '%s' cannot "
                                        "have a function as its value"\
                                        % (name, field))

        self.kwargs['show_default'] = self.kwargs.get('show_default', True)

        # default can be defined as a function
        default_field = self.kwargs.get('default')
        if callable(default_field) and not isinstance(default_field, DeployTimeField):
            self.kwargs['default'] = DeployTimeField(name,
                                                     param_type,
                                                     'default',
                                                     self.kwargs['default'],
                                                     return_str=True)

        # note that separator doesn't work with DeployTimeFields unless you
        # specify type=str
        self.separator = self.kwargs.pop('separator', None)
        if self.separator and not self.is_string_type:
            raise MetaflowException("Parameter *%s*: Separator is only allowed "
                                    "for string parameters." % name)

    def option_kwargs(self, deploy_mode):
        kwargs = self.kwargs
        if isinstance(kwargs.get('default'), DeployTimeField) and not deploy_mode:
            ret = dict(kwargs)
            help_msg = kwargs.get('help')
            help_msg = '' if help_msg is None else help_msg
            ret['help'] = help_msg + \
                "[default: deploy-time value of '%s']" % self.name
            ret['default'] = None
            ret['required'] = False
            return ret
        else:
            return kwargs

    def load_parameter(self, v):
        return v

    def _get_type(self, kwargs):
        default_type = str

        default = kwargs.get('default')
        if default is not None and not callable(default):
            default_type = type(default)

        return kwargs.get('type', default_type)

    @property
    def is_string_type(self):
        return self.kwargs.get('type', str) == str and\
               isinstance(self.kwargs.get('default', ''), strtype)

    # this is needed to appease Pylint for JSONType'd parameters,
    # which may do self.param['foobar']
    def __getitem__(self, x):
        pass


def add_custom_parameters(deploy_mode=False):
    '''Decorator for adding the main flow's Parameters' flags to the Metaflow CLI (cf. metaflow.cli.run)'''
    # deploy_mode determines whether deploy-time functions should or should
    # not be evaluated for this command
    return lambda cmd: add_custom_cmd_parameters(cmd, deploy_mode=deploy_mode)


def add_custom_cmd_parameters(cmd, deploy_mode=None):
    global _param_cmds, _param_opts, _parameters

    if _parameters is None:
        raise Exception(
            'Attempting to initialize Metaflow CLI before any flows have been processed (global '
            '`parameters` is None); avoid importing `metaflow.cli` until relevant flows have been '
            'declared/imported'
        )

    if _flow is None:
        raise Exception(
            'Attempting to initialize Metaflow CLI without knowing which flow will be run (_flow is None)'
        )
    if _flow not in _parameters:
        raise Exception(
            'Flow %s not found in global `_parameters` registry (%s)' % (
                _flow.__name__,
                ','.join([ flow.__name__ for flow in _parameters.keys() ]),
            )
        )

    if _param_opts is None:
        _param_opts = {}

    if cmd not in _param_opts:
        assert deploy_mode is not None
        _param_opts[cmd] = dict(deploy_mode=deploy_mode, opts=[])

    v = _param_opts[cmd]
    deploy_mode = v['deploy_mode']
    param_opts = v['opts']

    # if cmd was already present, it should be empty (⟹ a previous main flow's params have been cleared by
    # `clear_main_flow`)
    assert not param_opts

    # Iterate over parameters in reverse order so cmd.params lists options
    # in the order they are defined in the FlowSpec subclass
    params = list(_parameters[_flow].values())
    for param in params[::-1]:
        kwargs = param.option_kwargs(deploy_mode)
        opt = click.Option(('--' + param.name,), **kwargs)

        # Add this option to the command, as well as to our record of param-options we've added (for later removal, in
        # case a different flow is run)
        cmd.params.insert(0, opt)
        param_opts.insert(0, opt)

    return cmd


def set_parameters(flow, kwargs):
    seen = set()
    for var, param in flow._get_parameters():
        norm = param.name.lower()
        if norm in seen:
            raise MetaflowException("Parameter *%s* is specified twice. "
                                    "Note that parameter names are "
                                    "case-insensitive." % param.name)
        seen.add(norm)

    flow._success = True
    # Impose length constraints on parameter names as some backend systems
    # impose limits on environment variables (which are used to implement
    # parameters)
    parameter_list_length = 0
    num_parameters = 0
    for var, param in flow._get_parameters():
        k = param.name.replace('-', '_').lower()
        val = kwargs[k]
        # Account for the parameter values to unicode strings or integer
        # values. And the name to be a unicode string.
        parameter_list_length += len((param.name + str(val)).encode("utf-8"))
        num_parameters += 1
        # Support for delayed evaluation of parameters. This is used for
        # includefile in particular
        if callable(val):
            val = val()
        val = val.split(param.separator) if val and param.separator else val
        setattr(flow, var, val)
