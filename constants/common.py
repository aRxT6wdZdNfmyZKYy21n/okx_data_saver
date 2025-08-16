import typing


class CommonConstants(object):
    __slots__ = ()

    # A coroutine function with any args returning any awaitable result
    AsyncFunctionType = typing.Callable[
        ...,  # arbitrary positional/keyword parameters
        typing.Awaitable[typing.Any],
    ]
