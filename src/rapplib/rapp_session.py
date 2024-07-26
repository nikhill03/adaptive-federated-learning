"""
Provides common code for creating a session and performing CRUD operations.
The HTTP requests are blocking ones.
"""

import requests
import json

from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from requests.packages.urllib3.util.retry import Retry
from rapplib.metrics import MetricsCounter

# Constants
session_timeout = 5


def retryable_http(
    retries=5, backoff_factor=0.5, custom_headers: Optional[Dict[str, Any]] = None
):
    """
    Retryable HTTP request session
    :param retries: number of retry attempts to be performed
    :param backoff_factor: backoff factor in between each retry
    :param custom_headers: custom headers if any added to session
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 503, 504),
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    if custom_headers:
        session.headers.update(custom_headers)
    return session


def get_json(
    href: str, session: Session, exception, metrics_counters: MetricsCounter = None
):
    """
    Method to send HTTP Get requests to the given href.

    If there is a non-200 response, an exception containing the response is raised.

    :param href: endpoint to which the Get request has to be sent
    :param session: retryable HTTP session
    :param exception: exception to raise in case of an error
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response
    """
    try:
        response = session.get(href)
    except requests.exceptions.RequestException as e:
        raise exception(str(e)) from e

    if (response.status_code // 100) != 2:
        if metrics_counters is not None:
            metrics_counters.increment_http_error_response_counter(response.status_code)
        raise exception("Invalid response status received ", response)
    return response.json()


def put_json(
    href: str,
    session: Session,
    data: Dict[str, Any],
    exception,
    metrics_counters: MetricsCounter = None,
):
    """
    Method to send HTTP Put requests to the given href with data.

    If there is a non-200 response, an exception containing the response is raised.

    :param href: endpoint to which the Put request has to be sent
    :param session: retryable HTTP session
    :param data: data to be sent with the reqeust
    :param exception: exception to raise in case of an error
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response
    """
    json_data = json.dumps(data).encode("utf-8")
    session.headers.update({"content-type": "application/json"})
    try:
        response = session.put(url=href, data=json_data, timeout=session_timeout)
    except requests.exceptions.RequestException as e:
        raise exception(str(e)) from e

    if (response.status_code // 100) != 2:
        if metrics_counters is not None:
            metrics_counters.increment_http_error_response_counter(response.status_code)
        raise exception("Invalid response status received", response)

    try:
        results = response.json()
    except (json.JSONDecodeError, ValueError):
        return None

    return results


def post_json(
    href: str,
    session: Session,
    data: Dict[str, Any],
    exception,
    metrics_counters: MetricsCounter = None,
):
    """
    Method to send HTTP Post requests to the given href with data.

    If there is a non-200 response, an exception containing the response is raised.

    :param href: endpoint to which the Put request has to be sent
    :param session: retryable HTTP session
    :param data: data to be sent with the reqeust
    :param exception: exception to raise in case of an error
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response
    """
    json_data = json.dumps(data).encode("utf-8")
    session.headers.update({"content-type": "application/json"})
    try:
        response = session.post(url=href, data=json_data, timeout=session_timeout)
    except requests.exceptions.RequestException as e:
        raise exception(str(e)) from e

    if (response.status_code // 100) != 2:
        if metrics_counters is not None:
            metrics_counters.increment_http_error_response_counter(response.status_code)
        raise exception("Invalid response status received", response)

    try:
        results = response.json()
    except (json.JSONDecodeError, ValueError):
        return None

    return results


def patch_json(
    href: str,
    session: Session,
    data: Any,
    exception,
    metrics_counters: MetricsCounter = None,
) -> requests.Response:
    """
    Method to send HTTP Patch requests to the given href with data.

    If there is a non-200 response, an exception containing the response is raised.

    :param href: endpoint to which the Put request has to be sent
    :param session: retryable HTTP session
    :param data: data to be sent with the reqeust
    :param exception: exception to raise in case of an error
    :param metrics_counters: metrics counter object
    :return: json-encoded content of response
    """
    json_data = json.dumps(data).encode("utf-8")
    session.headers.update({"content-type": "application/json"})
    try:
        response = session.patch(url=href, data=json_data, timeout=session_timeout)
    except requests.exceptions.RequestException as e:
        raise exception(str(e), None) from e

    if (response.status_code // 100) != 2:
        if metrics_counters is not None:
            metrics_counters.increment_http_error_response_counter(response.status_code)
        raise exception("Invalid response status received", response)
    return response


def delete_request(href: str, session: Session, exception):
    """
    Method to send HTTP Delete request to the given href.

    If there is a non-200 response, an exception containing the response is raised.

    :param href: endpoint to which the Delete request has to be sent
    :param session: retryable HTTP session
    :param exception: exception to raise in case of an error
    """
    try:
        response = session.delete(href)
    except requests.exceptions.RequestException as e:
        raise exception(str(e)) from e

    if not response.ok:
        raise exception("Invalid response status received", response)
