#!/usr/bin/env python
"""
Design by Contract in Python.

Two module-level variables are available to control the behaviour:

1. C{ENABLED} - controls whether the whole functionality is enabled.
    The best use case is to write something like C{dbc.ENABLED = __debug__}
    after the first import and before the first use.
    Enabling it will consume more memory and CPU cycles
    for the wrapped function, so you should not enable it
    in the release builds.
    Default value it C{True}.
2. C{USE_EPYDOC_CACHE} - controls whether the epydoc linker is reused
    and cached between calls to contract_epydoc.
    This may occupy and even leak memory in rare cases (though it is obviously
    less important than enabling the whole functionality).
    Default value it C{True}.

@description: this project enables to use the basics of Design by Contract
    capabilities in Python, such as enforcing the contracts defined
    in the epydoc documentation.

@copyright: Alex Myodov <amyodov@gmail.com>

@url: http://code.google.com/p/python-dbc/
"""

__all__ = ("typed", "ntyped", "consists_of", "contract_epydoc")

import sys, inspect
from itertools import izip, chain
from functools import wraps
from types import NoneType, ClassType
from pprint import pprint


# Is the functionality enabled? May leak memory under load and heavy
# dynamic function construction, so better enable it only in release code.
ENABLED = False
# Are the epydoc parsers cached? May hog memory a bit.
USE_EPYDOC_CACHE = False


def typed(var, types):
    """
    Ensure that the C{var} argument is among the types passed as
    the C{types} argument.

    @param var: the argument to be typed.

    @param types: a tuple of types to check.
    @type types: tuple

    @returns: the var argument.

    >>> a = typed("abc", str)
    >>> type(a)
    <type 'str'>

    >>> b = typed("abc", int)  # doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    AssertionError: Value 'abc' of type <type 'str'> is not among
        the allowed types: <type 'int'>

    >>> c = typed(None, int)  # doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    AssertionError: Value None of type <type 'NoneType'> is not among
        the allowed types: <type 'int'>
    """
    assert isinstance(var, types), \
        "Value %r of type %r is not among the allowed types: %r" % (
            var, type(var), types)
    return var


def ntyped(var, types):
    """
    Ensure that the C{var} argument is among the types passed
    as the C{types} argument, or is C{None}.

    @param var: the argument to be typed.

    @param types: a tuple of types to check.
    @type types: tuple

    @returns: the C{var} argument.

    >>> a = ntyped("abc", str)
    >>> type(a)
    <type 'str'>

    >>> b = ntyped("abc", int)  # doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    AssertionError: Value 'abc' of type <type 'str'> is not among
        the allowed types: NoneType, <type 'int'>

    >>> c = ntyped(None, int)
    """
    assert var is None or isinstance(var, types), \
        "Value %r of type %r is not among the allowed types: NoneType, %r" % (
            var, type(var), types)
    return var


def consists_of(seq, types):
    """
    Check that the all elements from the C{seq} argument (sequence) are among
    the types passed as the C{types} argument.

    @param seq: the sequence which elements are to be typed.

    @param types: a tuple of types to check.
    @type types: tuple

    @return: whether the check succeeded.
    @rtype: bool

    >>> consists_of([5, 6, 7], int)
    True
    >>> consists_of([5, 6, 7, "abc"], int)
    False
    >>> consists_of([5, 6, 7, "abc"], (int, str))
    True
    """
    return all(isinstance(element, types) for element in seq)


def _get_function_base_path_from_stack(stack):
    """
    Given a stack (in the format as inspect.stack() returns),
    construct the path to the function (it may be a top-level in the module
    or defined in some deeper namespace, such as a class or another function).

    @param stack: The list similar in format to the result of inspect.stack().

    @return: The function fully qualified namespaces
        (with all intermediate namespaces where it is defined).
        The name of the function itself is not included.
    @rtype: basestring
    """
    base_function_list = [i[3] for i in reversed(stack)]
    # Start from a new module.
    if '<module>' in base_function_list:
        rindex = max(i
                         for i, v in enumerate(base_function_list)
                         if v == '<module>')
        del base_function_list[:rindex + 1]

    return '.'.join(base_function_list)


def _preprocess_field_argument(field_arg):
    """
    If the first line of the argument ends by ":" ("::" in the original code),
    it is dropped away.
    """
    # Drop the first line if ends with ":"
    lines = field_arg.split('\n')
    if lines and lines[0].endswith(':'):
        lines = lines[1:]
    # ... and recombine back.
    return '\n'.join(lines).lstrip()


def _parse_str_to_value(f_path, value_str, entity_name, _globals, _locals):
    """
    This function performs parsing of an argument.
    """
    try:
        expected_value = eval(value_str, dict(_globals), dict(_locals))
    except Exception, e:
        import traceback; traceback.print_exc()
        raise SyntaxError("%s:\n"
                          "The following %s "
                          "could not be parsed: %s\n" % (f_path,
                                                         entity_name,
                                                         value_str))
    return expected_value


def _parse_str_to_type(f_path, type_str, entity_name,
                       _globals=None, _locals=None):
    """
    @raises SyntaxError: If the string cannot be parsed as a valid type.
    """
    expected_type = _parse_str_to_value(f_path,
                                        type_str,
                                        "type definition for %s" % entity_name,
                                        _globals,
                                        _locals)

    if not isinstance(expected_type, (type, tuple, ClassType)):
        raise SyntaxError("%s:\n"
                          "The following type definition for %s "
                          "should define a type rather than a %s entity: "
                          "%s" % (f_path,
                                  entity_name,
                                  type(expected_type),
                                  type_str))

    return expected_type



def contract_epydoc(f):
    """
    The decorator for any functions which have a epydoc-formatted docstring.
    It validates the function inputs and output against the contract defined
    by the epydoc description.

    Currently, it supports the usual functions as well as simple object methods
    (though neither classmethods nor staticmethods).

    Inside the epydoc contracts, it supports the following fields:

    - C{@type arg:} - the type of the C{arg} argument is validated before
        the function is called.

    - C{@rtype:} - the return type of the function is validated after
        the function is called.

    - C{@precondition:} - the precondition (that may involve the arguments
        of the function)
        that should be satisfied before the function is executed.

    - C{@postcondition:} - the postcondition (that may involve the result
        of the function given as C{result} variable)
        that should be satisfied after the function is executed.

    @param f: The function which epydoc documentation should be verified.
    @precondition: callable(f)
    """
    if ENABLED:
        try:
            from epydoc import apidoc, docbuilder, markup
        except ImportError:
            raise ImportError(
                      "To use contract_epydoc() function, "
                          "you must have the epydoc module "
                          "(often called python-epydoc) installed.\n"
                      "For more details about epydoc installation, see "
                          "http://epydoc.sourceforge.net/")

        # Given a method/function, get the module
        # where the function is defined.
        module = inspect.getmodule(f)
        _stack = inspect.stack()[1:]

        # The function/method marked with @contract_epydoc may be
        # either top-level in the module,
        # or defined inside some namespace, like a class or another function.
        base_function_path = _get_function_base_path_from_stack(_stack)

        # Now, analyze the epydoc comments,
        # and maybe cacke the documentation linker.
        if hasattr(module, "_dbc_ds_linker"):
            _dbc_ds_linker = module._dbc_ds_linker
        else:
            _dbc_ds_linker = markup.DocstringLinker()
            if USE_EPYDOC_CACHE:
                module._dbc_ds_linker = _dbc_ds_linker


        contract = docbuilder.build_doc(f)

        preconditions = [description.to_plaintext(_dbc_ds_linker)
                             for field, argument, description in contract.metadata
                             if field.singular == "Precondition"]
        postconditions = [description.to_plaintext(_dbc_ds_linker)
                              for field, argument, description in contract.metadata
                              if field.singular == "Postcondition"]

        if isinstance(f, (staticmethod, classmethod)):
            raise NotImplementedError(
                      "The @contract_epydoc decorator is not supported "
                      "for either staticmethod or classmethod functions; "
                      "please use it before (below) turning a function into "
                      "a static method or a class method.")
        elif isinstance(contract, apidoc.RoutineDoc):
            f_path = "%(mod_name)s module (%(mod_file_path)s), %(func_name)s()" % {
                "mod_name"      : module.__name__,
                "mod_file_path" : contract.defining_module.filename,
                "func_name"     : ".".join(filter(None,
                                                  (base_function_path, f.__name__))),
                }
        else:
            raise Exception("@contract_epydoc decorator is not yet supported "
                            "for %s types!" % type(contract))

        _stack = inspect.stack()
        def_frame = _stack[1][0]

        # Don't copy the dictionaries, but refer to the original stack frame
        def_globals = def_frame.f_globals
        def_locals = def_frame.f_locals

        #
        # At this stage we have "f_path" variable containing
        # the fully qualified name of the called function.
        # Also, def_globals and def_locals contain the globals/locals of
        # the code where the decorated function was defined.
        #


        @wraps(f)
        def wrapped_f(*args, **kwargs):
            _stack = inspect.stack()

            # Do we actually want to use the globals with NoneType
            # already imported?
            #def_globals_with_nonetype = dict(def_globals);
            #def_globals_with_nonetype["NoneType"] = NoneType

            # Stack now:
            # [0] is the level inside wrapped_f.
            # [1] is the caller.

            # For "globals" dictionary, we should use the globals of the code
            # that called the wrapper function.
            call_frame = _stack[1][0]

            call_globals = call_frame.f_globals
            call_locals = call_frame.f_locals

            arguments_to_validate = list(contract.arg_types)

            try:
                expected_types = \
                    dict((argument,
                          _parse_str_to_type(
                              f_path,
                              _preprocess_field_argument(
                                  contract.arg_types[argument].to_plaintext(
                                      _dbc_ds_linker)),
                              "'%s' argument" % argument,
                              _globals=def_globals,
                              _locals=def_locals))
                             for argument in arguments_to_validate)
            except Exception, e:
                raise e

            # All values:
            # First try to use the default values;
            # then add the positional arguments,
            # then add the named arguments.
            values = dict(chain(izip(contract.posargs,
                                     ((df.pyval if df is not None else None)
                                          for df in contract.posarg_defaults)),
                                izip(contract.posargs, args),
                                kwargs.iteritems()))

            # Validate arguments
            for argument in arguments_to_validate:
                assert argument in values, "%r not in %r" % (argument, values)
                value = values[argument]
                expected_type = expected_types[argument]

                if not isinstance(value, expected_type):
                    raise TypeError("%s:\n"
                                    "The '%s' argument is of %r while must be of %r; "
                                    "its value is %r" % (f_path,
                                                         argument,
                                                         type(value),
                                                         expected_type,
                                                         value))

            # Validate preconditions.
            # Preconditions may use the globals from the function definition,
            # as well as the function arguments.
            locals_for_preconditions = values
            for description_str in preconditions:
                description_str = _preprocess_field_argument(description_str)
                value = _parse_str_to_value(f_path,
                                            description_str,
                                            'precondition definition',
                                            _globals=def_globals,
                                            _locals=locals_for_preconditions)
                if not value:
                    raise ValueError("%s:\n"
                                     "The following precondition results in logical False; "
                                     "its definition is:\n"
                                     "\t%s\n"
                                     "and its real value is %r" % (f_path,
                                                                   description_str.strip(),
                                                                   value))

            #
            # Call the desired function
            #
            result = f(*args, **kwargs)  # IGNORE THIS LINE

            # Validate return value
            if contract.return_type is not None:

                expected_type = _parse_str_to_type(
                                    f_path,
                                    _preprocess_field_argument(
                                        contract.return_type.to_plaintext(
                                            _dbc_ds_linker)),
                                    'return value',
                                    _globals=def_globals,
                                    _locals=values)
                if not isinstance(result, expected_type):
                    raise TypeError("%s:\n"
                                    "The following return value is of %r while must be of %r: "
                                    "%r" % (f_path,
                                            type(result),
                                            expected_type,
                                            result))

            # Validate postconditions.
            # Postconditions may use the globals from the function definition,
            # as well as the function arguments and the special "result" parameter.
            locals_for_postconditions = dict(locals_for_preconditions)
            locals_for_postconditions['result'] = result
            for description_str in postconditions:
                description_str = _preprocess_field_argument(description_str)
                value = _parse_str_to_value(f_path,
                                            description_str,
                                            'postcondition definition',
                                            _globals=def_globals,
                                            _locals=locals_for_postconditions)

                if not value:
                    raise ValueError("%s:\n"
                                     "The following postcondition results in logical False; "
                                     "its definition is:\n"
                                     "\t%s\n"
                                     "and its real value is %r" % (f_path,
                                                                   description_str.strip(),
                                                                   value))

            # Validations are successful
            return result

        return wrapped_f
    else:
        return f
