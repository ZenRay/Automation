#coding:utf8
"""Common Utility Functions.
"""
import inspect

from configparser import ConfigParser
from os import path

def check_function_arg(func, arg_name) -> bool:
    """Return True if `func` is callable and accepts a 'file' keyword argument.

    Rules:
    - If `func` is not callable, return False.
    - If inspect.signature() cannot obtain a signature (builtins/C functions), be permissive and return True.
    - If the function accepts **kwargs (VAR_KEYWORD), return True.
    - If the function has an explicit 'file' parameter that is not positional-only, return True.
    - Otherwise return False.
    """
    if not callable(func):
        return False

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        # Builtin or C extension where signature is not available; assume permissive
        return True

    params = sig.parameters
    # Accept functions that take **kwargs
    for p in params.values():
        if p.kind == inspect.Parameter.VAR_KEYWORD:
            return True

    # Accept if explicit 'arg_name' parameter exists and is not positional-only
    if arg_name in params and params[arg_name].kind != inspect.Parameter.POSITIONAL_ONLY:
        return True

    return False

    
def parse_conf(parser, file, template=None):
    """Parse Configuration File
    
    Parse the configuration file, if not exists, create one with the template.
    
    Args:
        parser (ConfigParser): ConfigParser instance.
        file (str): Configuration file path.
        template (str): Template content for the configuration file.
    """
    if not isinstance(parser, ConfigParser):
        raise TypeError(f"parser must be an instance of 'ConfigParser', get {type(parser)}")

    if not path.exists(file):
        with open(file, "w", encoding="utf8") as writer:
            if template is None:
                raise ValueError(
                    "Template must be provided when the configuration file does not exist."
                    " Get configuration file: {}".format(file)
                )
            writer.write(template)
    
    parser.read(file)