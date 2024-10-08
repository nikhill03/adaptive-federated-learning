"""
Provides common code for server routine of an rApp.

The server code provided here is based on Hypercorn
ASGI framework.
"""

import aiohttp
import asyncio
from contextlib import suppress
import logging
import json
import os

from typing import Any, Dict
from urllib import parse

from hypercorn.config import Config
from hypercorn.asyncio import serve
from hypercorn.middleware import DispatcherMiddleware
from functools import partial
from fastapi import status, APIRouter
from multiprocessing import Queue
from prometheus_client import make_asgi_app, multiprocess, CollectorRegistry

from rapplib.rapp import JobResultException
from rapplib import asyncio_logger

# Constants for rapp callback paths and job handlers
SERVER_LOG = None
# When job request data is retrieved it will either be process as a stream
# of line separated JSON objects or as a single JSON object.
# STREAMING_DATA_TYPES are those data types which return a stream.
# TODO: In the future this information will be within the data-type definition
# retrieved from Data Management and Exposure Services.
STREAMING_DATA_TYPES = ["pmhistory", "pmlive"]
# Of the data types that stream some leave the connection open forever and some
# close the connection. STREAM_WITH_END_TYPES incidates those data types that
# close.
# TODO: In the future this information will be within the data-type definition
# retrieved from Data Management and Exposure Services.
STREAM_WITH_END_TYPES = ["pmhistory"]
# DATA_IN_400 are those data types where there may be valid data in a 400
# response.
DATA_IN_400 = ["cmread", "cmwrite"]
rapp_job_result_url_path_base = "/job_results/"
rapp_job_status_url_path_base = "/job_statuses/"
rapp_job_result_url_route = "/job_results/{job_id}"
rapp_job_status_url_route = "/job_statuses/{job_id}"
job_result_notification = "JOB_RESULT"
job_status_notification = "JOB_STATUS"
job_result_processed = "JOB_PROCESSED"
generic_event = "GENERIC_EVENT"
queue_shutdown = "shutdown"
job_result_concurrency_default = 10
streaming_job_count_log_interval = 10000

# TODO - For the below router and mp Q should we allow
# access to dynamic object instances from the individual
# rapps using dependency injections of FastAPI ?

# Create the default router object
default_router = APIRouter()

# Job Handler Q
job_handler_q: Queue = Queue()

job_result_concurrency_cnt = 0
job_result_concurrency_limit = None


def make_partial(func, *args):
    """
    Return a new callable that acts like the func called
     with given arguments. Needed for pickling over mp Q.
    :param func: the function to be called
    :param args: the args to be passed to func
    """
    return partial(func, *args)


def generic_queue_request(event_type: str, data: Any):
    """
    Handler to be invoked for a generic event on the data process queue.
    :param event_type: the event type associated with the event. This will
                       be used to look up an appropriate handler for the event.
    :param data:  the (optional) data object associated with the event
    """

    return event_type, data


def job_result_handler(job_id: str, data_type: str, data):
    """
    Handler to be invoked when job result is available
    :param job_id: job id associated with the job
    :param data_type: the data type associated with the job result
    :param data: the data object associated with the job result
    """
    return job_id, data_type, data


def job_result_processing_done(job_id: str, data_type: str):
    """
    Handler to be invoked when job result is processed
    :param job_id: job id associated with the job
    :return:
    """
    return job_id, data_type


def job_status_handler(job_id: str, status: str):
    """
    Handler to be invoked when job status is available
    Currently nothing is done and we just return the job id, status.
    :param job_id: job id associated with the job
    :param status: status of the job
    """
    return job_id, status


class ResultsReceiver:
    """Base class for resource oriented and streaming oriented retrieval of
    job results.

    If there are multiple jobs (stream or resource) active at any time, a
    maximum of job_result_concurrency_limit will be processed.
    """

    def __init__(
        self,
        log,
        job_result_uri=None,
        job_id=None,
        data_type=None,
        method="GET",
        payload=None,
    ):
        self._log = log
        self.job_result_uri = job_result_uri
        self.job_id = job_id
        self.data_type = data_type
        self.method = method
        self.payload = payload
        self.session = self.get_session()

        self.job_failure = False
        self.count = 0

    def get_session(self):
        """Construct an appropriate session for this results receiver."""
        return None

    async def get(self):
        async with job_result_concurrency_limit:
            try:
                await self._get()
            except JobResultException as exc:
                self._log.error(
                    "Error while fetching %s results for job %s: %s",
                    self.data_type,
                    self.job_id,
                    exc,
                )
            except aiohttp.client_exceptions.ClientPayloadError as payload_exc:
                self._log.info(
                    "Exception %s, connection with %s stream is probably "
                    "lost. May reconnect",
                    payload_exc,
                    self.data_type,
                )
            except (
                aiohttp.client_exceptions.ServerDisconnectedError,
                aiohttp.client_exceptions.ClientConnectorError,
            ) as exc:
                self._log.exception("Exception %s, connection is terminated.", exc)
            except asyncio.CancelledError as exc:
                self._log.exception(
                    "Streaming for job %s cancelled [%s]", self.job_id, exc
                )
                raise
            except RuntimeError:
                # Catch RuntimeError separately so it is not swallowed. This may
                # happen during shutdown.
                raise
            except Exception as exc:
                # This will log _any_ exception, including the JobResultException
                # raised above when resp.status is non-success.  For the time
                # being this is all that happens: The exception is not "heard"
                # by any callers. This means that a developer investigating
                # problems will need to inspect the logs.
                self._log.exception("Exception while fetching job results: %s", exc)
        await self.finish()

    async def add_get_task(self):
        get_task = asyncio_logger.create_task(
            self.get(),
            logger=self._log,
            message="add_get_task raised an exception",
        )
        return get_task

    async def finish(self):
        """Empty to override as it's needed by pylint"""

    async def _get(self):
        """Empty to override as it's needed by pylint"""


class ResourceResultsReceiver(ResultsReceiver):
    """Asychronously handle non-streaming JSON results.

    The entire JSON object is processed as a block and sent down the
    job_handler_q queue to the data process.

    After the data is sent job_result_processed will be sent.
    """

    def get_session(self):
        client_args = {"trust_env": True}
        return aiohttp.ClientSession(**client_args)

    async def _get(self):
        async with self.session.request(
            self.method, self.job_result_uri, json=self.payload
        ) as resp:
            json_data = None
            if resp.status // 100 != 2:
                if not (self.data_type in DATA_IN_400 and resp.status == 400):
                    raise JobResultException(
                        "unexpected response for get job %s: %d with <%s>"
                        % (self.job_result_uri, resp.status, await resp.text())
                    )
                # If we got a 400 response and the response looks like a
                # cmmulti response, we want to continue, some of the results
                # may be good.
                json_data = await resp.json()
                if not valid_data_in_400_response(self.data_type, json_data):
                    raise JobResultException(
                        "unexpected response for get job %s: %d with <%s>"
                        % (self.job_result_uri, resp.status, json_data)
                    )

            # If we didn't already read the json body, get it now.
            if json_data is None:
                json_data = await resp.json()

            job_handler_q.put(
                (
                    job_result_notification,
                    make_partial(
                        job_result_handler,
                        self.job_id,
                        self.data_type,
                        json_data,
                    ),
                )
            )

    async def finish(self):
        # Notify that the json object for this response has been sent.
        job_handler_q.put(
            (
                job_result_processed,
                make_partial(job_result_processing_done, self.job_id, self.data_type),
            )
        )
        await self.session.close()
        self._log.info(
            "Processed job %s of type %s",
            self.job_id,
            self.data_type,
        )


class StreamingResultsReceiver(ResultsReceiver):
    """Asynchronously handle streaming JSON results.

    Each JSON object in the stream is processed individually and sent down
    the job_handler_q  queue to the data process.

    For data-types where the stream is expected to be endless (such as pmlive),
    the job will be restarted if there is an interruption.

    For other data-types, job_result_processed will be sent when all rows
    of data from a job have been sent.
    """

    def get_session(self):
        client_timeout = aiohttp.ClientTimeout(total=None)
        client_args = {"trust_env": True, "timeout": client_timeout}
        return aiohttp.ClientSession(**client_args)

    async def _get(self):
        async with self.session.request(
            self.method, self.job_result_uri, json=self.payload
        ) as resp:
            if resp.status // 100 != 2:
                # If the response was non-success raise an exception to
                # break out of this request. Set job_failure to true so
                # that pmlive will not try again.
                self.job_failure = True
                raise JobResultException(
                    "unexpected response for get job %s: %d with <%s>"
                    % (self.job_result_uri, resp.status, await resp.text())
                )
            self._log.info(
                "Getting %s results from %s", self.data_type, self.job_result_uri
            )
            async for line in resp.content:
                data = json.loads(line)

                def put_queue():
                    job_handler_q.put(
                        (
                            job_result_notification,
                            make_partial(
                                job_result_handler,
                                self.job_id,
                                self.data_type,
                                data,
                            ),
                        )
                    )

                put_queue()
                self.count = self.count + 1
                if (
                    self.count > 0
                    and self.count % streaming_job_count_log_interval == 0
                ):
                    self._log.info(
                        "Processed %d lines of %s from %s",
                        self.count,
                        self.data_type,
                        self.job_result_uri,
                    )
                # potentially yield to event loop
                try:
                    await asyncio.sleep(0)
                except asyncio.CancelledError:
                    self._log.info(
                        "sleep in streaming job %s cancelled, ignoring",
                        self.job_result_uri,
                    )

    async def finish(self):
        # If this data type is supposed to stay alive forever (pmlive), then
        # when the connection finished, make another one.
        if self.data_type not in STREAM_WITH_END_TYPES and not self.job_failure:
            self._log.info(
                "Retrying to get % results data for %s", self.data_type, self.job_id
            )
            task = self.add_get_task()
            await task
        else:
            # Otherwise, notify that all the json lines for this response
            # have been sent.
            def notify_done():
                job_handler_q.put(
                    (
                        job_result_processed,
                        make_partial(
                            job_result_processing_done, self.job_id, self.data_type
                        ),
                    )
                )

            self._log.info("sending job result processed for %s", self.job_result_uri)
            notify_done()
            await self.session.close()

            result_logger = self._log.info
            if self.count == 0:
                result_logger = self._log.warning
            result_logger(
                "Result count of %d for job %s of type %s",
                self.count,
                self.job_id,
                self.data_type,
            )


def valid_data_in_400_response(data_type, json_data):
    """
    Check to see if a response body looks like a consumable response, even
    though it is a status 400.
    """
    # TODO: Current this only cares about cmwrite and cmread, but
    # we should make this dispatchable when we need more.
    try:
        _ = json_data[0]["result"]
    except (IndexError, KeyError):
        return False
    return data_type in ("cmwrite", "cmread")


@default_router.post(rapp_job_result_url_route, status_code=status.HTTP_200_OK)
async def job_result(job_id: str, job_result: Dict):
    """
     Route for job results. It will pass the handler
     to another process over mp Q which will execute it.
    :param job_id: job id associated with the job
    :param job_result: request body containing the info on job result
    """
    log = SERVER_LOG or logging.getLogger("job_result")
    data_type = job_result["data"]["data_type"]
    job_result_uri = job_result["data"]["uri"]

    if data_type not in STREAMING_DATA_TYPES:
        session = ResourceResultsReceiver(
            log, job_id=job_id, job_result_uri=job_result_uri, data_type=data_type
        )
    else:
        # If there is an exception in the receiving task,
        # StreamingResultsReceiver class will retry with a new task, for data
        # types are not STREAM_WITH_END_TYPES (e.g., pmlive).
        session = StreamingResultsReceiver(
            log, job_id=job_id, job_result_uri=job_result_uri, data_type=data_type
        )

    if job_result_concurrency_limit.locked():
        log.info(
            "Job_ID %s: Reached concurrency limit of %d. "
            "Job result will be processed once context is available.",
            job_id,
            job_result_concurrency_cnt,
        )
    task = session.add_get_task()
    await task


@default_router.post(rapp_job_status_url_route, status_code=status.HTTP_200_OK)
async def job_status(job_id: str, job_status: Dict):
    """
     Route for job status. It will pass the handler
     to another process over mp Q which will execute it.
    :param job_id: job id associated with the job
    :param job_status: status of the job
    :return:
    """
    job_handler_q.put(
        (
            job_status_notification,
            make_partial(
                job_status_handler, job_id, job_status["job_status_notification"]
            ),
        )
    )


class Server:
    """
    The server class provides all the members and
    methods to work on the Hypercorn ASGI framework.
    """

    def __init__(
        self,
        service_url: str = "http://0.0.0.0:8080",
        service_log: logging = None,
    ):
        self.shutdown_event = asyncio.Event()
        self.service_url: str = service_url
        self.task_started: bool = False
        self.task: asyncio.Future = None
        if service_log is None:
            self.log = logging.getLogger("rapp_service")
        else:
            self.log = service_log
        # Set SERVER_LOG so data handling routines are sharing the
        # pre-configured log.
        global SERVER_LOG
        SERVER_LOG = self.log
        self.app_dict: dict = {}
        self._init_host_port()
        # Make prometheus metrics multiple process capable.
        self.registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(self.registry)

        global job_result_concurrency_cnt
        job_result_concurrency_cnt = int(
            os.environ.get(
                "JOB_RESULT_CONCURRENCY_LIMIT",
                job_result_concurrency_default,
            )
        )
        global job_result_concurrency_limit
        job_result_concurrency_limit = asyncio.Semaphore(
            value=job_result_concurrency_cnt
        )

    def _init_host_port(self) -> None:
        parsed_url = parse.urlparse(self.service_url)
        if ":" in parsed_url.netloc:
            self.port: str = parsed_url.netloc.split(":")[1]
        elif parsed_url.scheme == "https":
            self.port: str = "443"
        else:
            self.port: str = "80"
        # Host is always 0.0.0.0 because we always bind there.
        self.host = "0.0.0.0"

    def add_app(self, route: str, app) -> None:
        if app is None:
            return
        self.app_dict[route] = app

    def add_metrics_app(self, app) -> None:
        """Add the handler for getting prometheus metrics at /metrics."""
        if app is None:
            return

        def get_metrics_app():
            return make_asgi_app(registry=self.registry)

        # MetricsInternalRedirect is required to make sure that we are able
        # use the metrics app without causing clients to redirect, and while
        # logging reasonable log messages.
        app.add_middleware(MetricsInternalRedirect)
        app.mount("/metrics", get_metrics_app())

    async def stop(self) -> None:
        if self.task_started:
            self.log.info("starting server task shut down")
            self.task_started = False
            self.shutdown_event.set()
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
                self.log.info("finished shutting down the server task")

    def start(self) -> None:
        if not self.task_started:
            if len(self.app_dict) == 0:
                self.log.error("Apps not set for server")
                return

            config = Config()
            config.bind = [self.host + ":" + self.port]
            # Access log to stdout.
            config.accesslog = "-"
            config.backlog = 1000
            config.max_app_queue_size = 1000
            # Start the server
            self.log.info(
                "starting the server task host(%s), port(%s)",
                self.host,
                self.port,
            )
            # strip_lifespan_events is required because we are not able to
            # use modern Python and thus cannot upgrade to modern fast api
            # and hypercorn. Without those, asgi lifespan events cause alarming
            # log message during such events, notably shutdown, clouding the
            # logs.
            # TODO: Upgrade to modern Python!
            self.task = asyncio_logger.create_task(
                serve(
                    strip_lifespan_events(DispatcherMiddleware(self.app_dict)),
                    config,
                    shutdown_trigger=self.shutdown_event.wait,
                ),
                logger=self.log,
                message="Server raised an exception",
            )
            self.task_started = True


def strip_lifespan_events(app):
    """
    handles/ignores lifespan events from being routed to the given app.
    """

    # via: https://gist.github.com/sheenobu/064b501b76415c6f6dacb6afa1ac6305
    async def _app(scope, receive, send):
        if scope.get("type") == "lifespan":
            payload = await receive()
            await send({"type": payload["type"] + ".complete"})
            return
        await app(scope, receive, send)

    return _app


class MetricsInternalRedirect:
    """
    Redirect metrics so that we can effectively log when we access metrics.
    """

    # NOTE: This deals with what appears to be a bug or misfeature in FastAPI.
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("path") == "/metrics":
            scope["path"] = "/metrics/metrics"
            scope["raw_path"] = b"/metrics/metrics"
        await self.app(scope, receive, send)
