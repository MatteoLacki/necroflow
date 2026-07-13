import sys

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup
else:
    from exceptiongroup import ExceptionGroup


__all__ = ["ExceptionGroup"]
