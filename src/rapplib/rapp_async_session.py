"""
Provides common code for creating an asynchronous session and performing CRUD operations
"""

from typing import Dict, Any, Optional
from rapplib.metrics import MetricsCounter
from aiohttp import ClientSession as Session
from aiohttp import ClientResponse as Response


def get_session(custom_headers: Optional[Dict[str, Any]] = None):
    return Session(headers=custom_headers)


async def handle_response(
    response: Response, exception=None, metrics_counters: MetricsCounter = None
):
    """
    Checks the response of an HTTP request and updates the metrics if provided.

    If there is a non-200 response, an exception containing the response is raised.

    :param exception: exception to raise in case of an error. Set it to None if
                      caller needs different error handling.
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response and the status code
    """
    async with response:
        if (response.status // 100) != 2:
            if metrics_counters is not None:
                metrics_counters.increment_http_error_response_counter(response.status)
            text = await response.text()
            if exception is not None:
                # caller might want different handling if exception is None
                raise exception(
                    "Invalid response status received ", response.status, text
                )

        json_results = await response.json(content_type=None)
        return json_results, response.status


async def get_json(
    session: Session, href: str, exception=None, metrics_counters: MetricsCounter = None
):
    """
    Method to send HTTP Get requests to the given href.

    If there is a non-200 response, an exception containing the response is raised.

    :param session: async HTTP session
    :param href: endpoint to which the Get request has to be sent
    :param exception: exception to raise in case of an error. Set it to None if
                      caller needs different error handling.
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response and the status code
    """
    response = await session.get(href)
    return await handle_response(response, exception, metrics_counters)


async def put_json(
    session: Session,
    href: str,
    data: Dict[str, Any],
    exception=None,
    metrics_counters: MetricsCounter = None,
):
    """
    Method to send HTTP Put requests to the given href with data.

    If there is a non-200 response, an exception containing the response is raised.

    :param session: async HTTP session
    :param href: endpoint to which the Put request has to be sent
    :param data: data to be sent with the reqeust
    :param exception: exception to raise in case of an error. Set it to None if
                      caller needs different error handling.
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response and the status code
    """
    response = await session.put(url=href, json=data)
    return await handle_response(response, exception, metrics_counters)


async def post_json(
    session: Session,
    href: str,
    data: Dict[str, Any],
    exception=None,
    metrics_counters: MetricsCounter = None,
):
    """
    Method to send HTTP Post requests to the given href with data.

    If there is a non-200 response, an exception containing the response is raised.

    :param session: async HTTP session
    :param href: endpoint to which the Put request has to be sent
    :param data: data to be sent with the reqeust
    :param exception: exception to raise in case of an error. Set it to None if
                      caller needs different error handling.
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response and the status code
    """
    response = await session.post(href, json=data)
    return await handle_response(response, exception, metrics_counters)


async def patch_json(
    session: Session,
    href: str,
    data: Any,
    exception=None,
    metrics_counters: MetricsCounter = None,
):
    """
    Method to send HTTP Patch requests to the given href with data.

    If there is a non-200 response, an exception containing the response is raised.

    :param session: async HTTP session
    :param href: endpoint to which the Put request has to be sent
    :param data: data to be sent with the reqeust
    :param exception: exception to raise in case of an error. Set it to None if
                      caller needs different error handling.
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response and the status code
    """
    response = await session.patch(href, json=data)
    return await handle_response(response, exception, metrics_counters)


async def delete_request(session: Session, href: str, exception=None):
    """
    Method to send HTTP Delete request to the given href.

    If there is a non-200 response, an exception containing the response is raised.

    :param session: async HTTP session
    :param href: endpoint to which the Delete request has to be sent
    :param exception: exception to raise in case of an error. Set it to None if
                      caller needs different error handling.
    :return: json-encoded content of response and the status code
    """
    response = await session.delete(href)
    return await handle_response(response, exception, None)
