"""
asyncio_logger provides a way to create an asyncio task ensuring an exception
handler and an appropriate logger.
"""
from typing import Any, Awaitable, Optional, TypeVar, Tuple

import asyncio
import functools
import logging


T = TypeVar("T")


def create_task(
    coroutine: Awaitable[T],
    *,
    logger: logging.Logger,
    message: str = "",
    message_args: Tuple[Any, ...] = (),
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> "asyncio.Task[T]":  # This type annotation has to be quoted for Python < 3.9,
    # see https://www.python.org/dev/peps/pep-0585/
    """
    This helper function wraps a ``loop.create_task(coroutine())`` call and
    ensures there is an exception handler added to the resulting task. If the
    task raises an exception it is logged using the provided ``logger``, with
    additional context provided by ``message`` and optionally ``message_args``.

    :param coroutine: Asyncio coroutine
    :param logger: custom logger object of the 'logging' class
    :param message: String object which can provide more context to the
                    exception if needed
    :param message args: Optional (args passed to the message)
    :param loop: Optional (uses default event loop)
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    task = loop.create_task(coroutine)
    task.add_done_callback(
        functools.partial(
            _handle_task_result,
            logger=logger,
            message=message,
            message_args=message_args,
        )
    )
    return task


def _handle_task_result(
    task: asyncio.Task,
    *,
    logger: logging.Logger,
    message: str = "",
    message_args: Tuple[Any, ...] = (),
) -> None:
    """
    :param task: wrapped coroutine
    :param logger: custom logger object of the 'logging' class
    :param message: String object which can provide more context to the
                    exception if needed
    :param message args: Optional (args passed to the message)
    """
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        # Here we log the exception to be sure we do, but then we re-raise it
        # to make sure something else becomes aware of it.
        # Add to the message string so exc is accepted as part of the message.
        message = message + ": %s"
        logger.exception(message, *message_args, exc)
        raise
