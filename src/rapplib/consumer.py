"""
Consumer provides the base class for an rApp that is a consumer. Implementing
rApps may subclass Consumer and override functionality as required.
"""

import os
import asyncio
from contextlib import suppress
from enum import Enum
import multiprocessing
import signal
import sys
import time
from typing import NamedTuple
import queue
import uuid

from fastapi import FastAPI, status
import requests

from rapplib import rapp, server, heartbeat, metrics, asyncio_logger
from rapplib.server import (
    job_result_notification,
    job_result_processed,
    job_status_notification,
    generic_event,
    queue_shutdown,
)


# Constants

# seconds
JOB_FREQUENCY_DEFAULT = 600
# minutes
PM_DATA_WINDOW_START_DEFAULT = 15
VMWARE = "VMware"
# seconds
CM_REQUEST_FREQUENCY_DEFAULT = "900"
# disabled by default
PM_CYCLES_FOR_JOBS_DEFAULT = -1
# disabled by default
STOP_RAPP_WHEN_JOB_REGISTRATIONS_STOP_DEFAULT = "false"

JOB_DATA_TYPES = [
    rapp.pm_history_info_type,
    rapp.pm_live_info_type,
    rapp.cmwrite_info_type,
    rapp.topologyservice_info_type,
    rapp.cmread_info_type,
    rapp.cmtrack_info_type,
]

# All the data types which rApp should keep the job to receive multiple change
# notifications as job results
# TODO:: We should make the value of this variable be filled by
#  querying the subscription data-types that the dms hosts,
#  and looking at a piece of metadata which is available in the data-type definition.
CONTINUOUS_SUBSCRIPTION_DATA_TYPES = [
    rapp.cmtrack_info_type,
    rapp.topologyservice_info_type,
]

# Reverse Job Handler Q
reverse_job_handler_q: multiprocessing.Queue = multiprocessing.Queue()


class Reverse_Queue(Enum):
    """An enumeration of the types of events that may be sent on the reverse
    queue."""

    reverse_handler = "reverse_handler"
    queue_shutdown = "shutdown"


class Reverse_Handlers(Enum):
    """An enumeration that maps data-type to a reverse handler."""

    cmread_direct = "cmread_direct_reverse_handler"
    cmwrite_direct = "cmwrite_direct_reverse_handler"
    cmwrite = "cmwrite_reverse_handler"
    topologyservice = "topologyservice_reverse_handler"
    cmread = "cmread_reverse_handler"


class DataWindow(NamedTuple):
    """A tuple indicating a start and end time window for job."""

    start: int
    end: int


class Consumer:
    """
    A consumer is an rApp that uses data provided by other services in the R1
    environment.

    A consumer rApp may subclass Consumer and override methods and class
    variables to achieve desired functionality.

    Every subclass must set the following class variables to effectively
    register and onboard the rApp and retrieve data.

    * service_name: The name by which the rApp will be known in the network.
    * service_prefix: The base path of URLs for the HTTP server.
    * service_version: For example "1.1.2".
    * service_display_name: The readable name for the rApp. May be the same as
                            service_name.
    * service_description: A brief description of what the rApp does.
    * vendor: Who has provided the rApp.

    Optionally they may set:
    * service_port: The port on which the HTTP server required for receiving
                    notifications listens. Defaults to 8080.

    There are several other class variables that may be changed to achieve
    different behavior. Inspect the code comments for more details.
    TODO: When we switch to sphinx we can more cleanly document class variables.
    """

    # Override these variables in a subclass, see descriptions in docstring
    # above.
    service_name = "set_me_in_class"
    service_port = "8080"
    service_prefix = "/v0alpha1"
    service_version = "0.1.0"
    service_display_name = "Set me in class"
    service_description = (
        "A python consumer rApp that needs to set its service_description"
    )
    vendor = VMWARE

    # the following environment variables are used solely in testing.
    # They exist so bootstrapper and dms registrations can be bypassed
    # for testing purposes.
    testing_mode = False
    testing_cmread_uri = ""
    testing_consumer_href = ""

    # data_type_handler_map associates the name of a data-type that may be
    # consumed via job creation on the dms with the name of a function that
    # will handle the data. The handlers will be passed a job_id, the data-type
    # and a single data item. What that data item is depends on the
    # content-type of the jobs results. If it is "application/json" it will be
    # the entire JSON object (decoded from JSON). If it is streaming
    # (line-separated) JSON it will be one row of the response.
    #
    # rApps may extend or override this map to deal with other formats. Any
    # named method must be implemented on the rApp class or its supers or
    # an exception will be raised.
    #
    # Jobs can be created with the register_job method (below).
    data_type_handler_map = {
        "pmhistory": "pmhistory_handler",
        "cmwrite": "cmwrite_handler",
        "topologyservice": "topologyservice_handler",
        "cmread": "cmread_handler",
        "cmtrack": "cmtrack_handler",
    }

    # data_type_reverse_handler_map associates the name of a data-type that may
    # be consumed by reverse handlers . The handlers will be passed a data-type
    # and a data item. What that data item is depends on the rever handler and
    # it implementation.
    #
    # rApps may extend or override this map to deal with other formats. Any
    # named method must be implemented on the rApp class or its supers or
    # an exception will be raised.
    data_type_reverse_handler_map = {
        Reverse_Handlers.cmread_direct.name: Reverse_Handlers.cmread_direct.value,
        Reverse_Handlers.cmread.name: Reverse_Handlers.cmread.value,
        Reverse_Handlers.cmwrite.name: Reverse_Handlers.cmwrite.value,
        Reverse_Handlers.cmwrite_direct.name: Reverse_Handlers.cmwrite_direct.value,
        Reverse_Handlers.topologyservice.name: Reverse_Handlers.topologyservice.value,
    }

    # The default, in seconds, at which we will make regular job queries.
    job_frequency = JOB_FREQUENCY_DEFAULT

    # The default number of minutes before a given time that will be the
    # "start" time for a PM history query.
    pm_data_window_start = PM_DATA_WINDOW_START_DEFAULT

    # If pmhistory_counter_names or pmhistory_counter_names and
    # pmhistory_cell_ids have lengths then we will run a pmhistory job at the
    # intervals described by job_frequency and pm_data_window_start.
    pmhistory_counter_names = []
    pmhistory_cell_ids = []

    # Initial data gather allows us to make a first data request for multiple
    # data-types using the provide time window and the "standard" query class
    # variables used for the later regular jobs.
    initial_data_gather = {
        "pmhistory": DataWindow(0, 0),
    }

    # If cmread_defs has length, then Consumer will do cm read for
    # these jobs definitions every CM_REQUEST_FREQUENCY seconds.
    # Format is [{cell_id1, parameter_name1}, {cell_id2, parameter_name2}, ...]
    cmread_defs = []

    # topologyservice job will be registered if topologyservice_defs is not empty.
    # A topologyservice consumer will receive notification when an update
    # to topology information is available. Notifications are received until job
    # is deleted.
    topologyservice_defs = {}

    # subscription_job_ids is a list of active subscriptions which should be
    # deleted at shutdown time
    subscription_job_ids = []

    # cmtrack job id needs to be saved,
    # so it can only be cleared when a new job request is sent or app cleanup happens
    cmtrack_job_id = ""

    # @param use_data_process is True when a secondary process with a queue
    # is needed.
    def __init__(self, logger, use_data_process=True, self_url=None):
        """Initializes but does not start a Consumer.

        :param logger: A logging.Logger created in the caller. Required.
        :param use_data_process: If False, the secondary process for data
                                 processing will not be started.
        :param self_url: If provided, the HTTP URL where this rApp is listening.
                         If not set the value of the SELF_URL environment variable
                         will be used. If that is not set the service name and
                         port will be used to generate a value. In most situations
                         SELF_URL should be set in the environment.
        """
        self.log = logger
        self.cleaning_up = False
        self.use_data_process = use_data_process
        self._exiting = False

        if self_url is not None:
            self.service_self_url = self_url + self.service_prefix
        else:
            self.service_self_url = (
                os.environ.get("SELF_URL", self.self_url_default) + self.service_prefix
            )
        # Override job_frequency if JOB_FREQUENCY is set.
        if os.environ.get("JOB_FREQUENCY"):
            self.job_frequency = int(os.environ.get("JOB_FREQUENCY"))
        if os.environ.get("WINDOW_SIZE"):
            self.pm_data_window_start = int(os.environ.get("WINDOW_SIZE"))

        self.cm_data_job_freq = int(
            os.environ.get("CM_REQUEST_FREQUENCY", CM_REQUEST_FREQUENCY_DEFAULT)
        )

        if os.environ.get("TESTING_MODE"):
            self.testing_mode = os.environ.get("TESTING_MODE").lower() == "true"
            self.testing_cmread_uri = os.environ.get("TESTING_CMREAD_URI", "")
            self.testing_consumer_href = os.environ.get("TESTING_CONSUMER_HREF", "")

        self.pm_cycles_for_jobs = int(
            os.environ.get("PM_CYCLES_FOR_JOBS", PM_CYCLES_FOR_JOBS_DEFAULT)
        )
        self.stop_rapp_when_job_registrations_stop = (
            os.environ.get(
                "STOP_RAPP_WHEN_JOB_REGISTRATIONS_STOP",
                STOP_RAPP_WHEN_JOB_REGISTRATIONS_STOP_DEFAULT,
            ).lower()
            == "true"
        )
        if self.pm_cycles_for_jobs > -1:
            self.log.info(
                "Job registration will happen for %d PM cycles", self.pm_cycles_for_jobs
            )
        else:
            self.log.info("Job registration won't stop")
        if self.stop_rapp_when_job_registrations_stop:
            self.log.info(
                "rApp will stop as soon as the configured %d number"
                " of PM cycles is reached",
                self.pm_cycles_for_jobs,
            )

        # A map of discoverd services and their URLs
        self.service_map = {}

        # The web server that an rApp must run to receive job notifications.
        self.server = server.Server(
            service_url=self.service_self_url, service_log=self.log
        )

        # The heartbeat_manager automatically sends liveness messages to the
        # service registry at intervals determined by the registry.
        self.heartbeat_manager: heartbeat.HeartbeatManager = None

        if self.use_data_process:
            # The data_process is a second process started by the rApp to
            # handle data processing in a separate process so that CPU
            # intensive processing does not block the input/output processing
            # that happens in the first process. Data is provided to the second
            # process over the data_process_q which is read in an endless loop
            # in data_handler_process_exec
            self.data_process: multiprocessing.Process = None
            self.data_process_q: multiprocessing.Queue = server.job_handler_q

            # The reverse_data_process is an asyncio task started by the rApp.
            # The queue provides a mechanism whereby data_process can cause
            # additional data gathering actions (I/O) in the main (async) process.
            self.reverse_data_process_q: multiprocessing.Queue = reverse_job_handler_q
            self._reverse_queue_ticker_task: asyncio.Future = None

        # The default prometheus metrics the rApp will produce, keeping track
        # of service registration and HTTP errors. An rApp may also choose to
        # add any additional prometheus metrics in its own __init__.
        self.metrics_counters = metrics.MetricsCounter(self.service_name)
        self.custom_headers = {"user-agent": self.service_user_agent}

        # Keep a reference of the job ticker as that is what keeps the
        # application running and allows it to be shut down easily (by
        # cancelling the task).
        self._job_ticker_task: asyncio.Future = None

        # Keep a reference of the cmread ticker
        self._cmread_ticker_task: asyncio.Future = None

        self.stop_event = multiprocessing.Event()

        # count how many pm cycles job registrations happen for, so it can
        # be compared with the configurable self.pm_cycles_for_jobs.
        self.pm_cycles_done = 0

    @property
    def self_url_default(self):
        """
        The default url of this service, created from the service_name and
        service_port. This may be overridden by setting SELF_URL in the
        environment.
        """
        return "http://" + self.service_name + ":" + self.service_port

    @property
    def service_user_agent(self):
        """
        Provides a user-agent string that is unique to the rApp which is useful
        for processing logs.
        """
        return self.service_name + "/" + self.service_version

    @property
    def prometheus_counter_labels(self):
        """
        The default labels that must be included with an outgoing prometheus
        metric that is a counter.
        """
        return {
            "metric_type": "Counter",
            "source_type": "RAPP",
            "source_id": "self.service_name",
            "source_vendor": "self.vendor",
        }

    @property
    def prometheus_gauge_labels(self):
        """
        The default labels that must be included with an outgoing prometheus
        metric that is a gaugue.
        """
        return {
            "metric_type": "Gauge",
            "source_type": "RAPP",
            "source_id": "self.service_name",
            "source_vendo": "self.vendor",
        }

    async def new_rapp_session(self):
        """Starts all required asyncio tasks to start and run rApp and then
        awaits their completion.

        May be overridden if required, but in some cases it will make more
        sense to override start_tasks (to start additional tasks).
        """
        await self.start_tasks()
        await self.await_tasks()

    def loop_exception_handler(self, loop, context):
        """
        Once start_tasks is called, we want to globally catch any unhandled
        exceptions and cause the application to exit.
        """
        # NOTE: For developers this means that it is very important to locally
        # handle exceptions.
        exc = context.get("exception", None)
        if (
            not self._exiting
            and exc is not None
            and exc is not isinstance(exc, SystemExit)
        ):
            # Don't use exception here to avoid failures during memory
            # problems.
            self.log.error(
                "Loop exception handler got exception: %s[%s]",
                exc.__class__.__name__,
                exc,
            )
            self._exiting = True
            try:
                asyncio_logger.create_task(self.cleanup(), logger=self.log)
                tasks = [
                    task
                    for task in asyncio.all_tasks()
                    if task is not asyncio.current_task()
                ]
                asyncio.gather(*tasks)
            except asyncio.CancelledError:
                # TODO: Figure out if pass is correct here.
                # See https://jira.eng.vmware.com/browse/CRIC-2042
                pass
            except RuntimeError:
                # no current loop
                pass

            def exit():
                sys.exit(1)

            try:
                loop.call_soon(exit)
            except RuntimeError:
                # no current loop already exiting
                pass

    async def start_tasks(self):
        """
        Start all common tasks and processes.

        Tasks include initialization tasks, such as contacting the bootstrapper
        and doing service registry, and ongoing (repeated) tasks for retrieving
        data.

        Override this to add additional tasks that are run by the rApp, making
        sure to call super() to perform necessary onboarding tasks.
        """
        self.log.info("Service registration and task initialization started")

        # 0. Make sure we can catch exceptions.
        event_loop = asyncio.get_running_loop()
        event_loop.set_exception_handler(self.loop_exception_handler)

        if not self.testing_mode:
            # 1. Bootstrap to fetch the service registry
            try:
                self.get_registry_href()
            except (rapp.RappError, ValueError, KeyError) as exc:
                self.log.exception("Unable to bootstrap rapp: %s", exc)
                await self.cleanup()
                return

            # 2. Register rapp into the service registry
            try:
                heartbeat_timer = await self.register_with_service_registry()
            except (rapp.RappError, ValueError, KeyError) as exc:
                self.log.exception("Unable to register rapp: %s", exc)
                await self.cleanup()
                return
            self.log.info("rApp registered to DMS")

        # 3. Fill in the service map
        try:
            self.update_service_map()
        except (rapp.RappError, ValueError, KeyError) as exc:
            self.log.exception("Unable to update service map: %s", exc)
            await self.cleanup()
            return
        self.log.info("Updated service map")

        # 4. Start the data handler before signal handling and web server.
        if self.use_data_process:
            self.start_data_process()

        # 5. Register a signal handler (in the parent) to shut things down
        # cleanly.
        for signame in ["SIGINT", "SIGCHLD", "SIGTERM"]:
            event_loop.add_signal_handler(
                getattr(signal, signame),
                lambda: asyncio_logger.create_task(
                    self._signal_handler(signame),
                    logger=self.log,
                    message="Task raised an exception",
                ),
            )

        # 6. Start the reverse queue job for getting messages from the data
        # process.
        if self.use_data_process:
            self._reverse_queue_ticker_task = asyncio_logger.create_task(
                self.reverse_queue_ticker(),
                logger=self.log,
                message="Task raised an exception",
            )

        # 7. Setup the web server and app, and start it.
        self.start_server()
        # Do a brief sleep to yield back to the server so that it finishes
        # starting up. If we do not jobs will often be registered prior
        # to the server being ready.
        await asyncio.sleep(1)

        if not self.testing_mode:
            # 8. Start the liveness heartbeat.
            await self.start_heartbeat(heartbeat_timer)

            # 9. Register initial jobs.
            try:
                self.register_initial_jobs_with_consumer_service()
            except (rapp.RappError, ValueError, KeyError) as exc:
                self.log.exception("Unable to register jobs with DMS: %s", exc)
                await self.cleanup()
                return
            except Exception as exc:
                self.log.exception(
                    "Encountered an exception: %s. Gracefully terminate the rapp",
                    exc,
                )
                await self.cleanup()
                return

        # 10. Start periodic CM read request tickers
        try:
            self._cmread_ticker_task = asyncio_logger.create_task(
                self.start_cmread_ticker(),
                logger=self.log,
                message="Task raised an exception",
            )
        except (rapp.RappError, ValueError, KeyError) as exc:
            self.log.exception("Unable to start cmread ticker: %s", exc)
            await self.cleanup()
            return

        if not self.testing_mode:
            # 11. Start the periodic job request ticker and await its completion.
            self._job_ticker_task = asyncio_logger.create_task(
                self.start_job_ticker(),
                logger=self.log,
                message="Task raised an exception",
            )
        else:
            # register one job to signal that app is live
            self.log.info("sending one job to signal test framework")
            self.register_jobs_with_consumer_service()

    async def await_tasks(self):
        """Await all tasks to allow the rApp to cleanly shutdown."""
        tasks = [
            task for task in asyncio.all_tasks() if task is not asyncio.current_task()
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.log.info("all tasks now cancelled")
        except Exception as exc:
            self.log.exception(
                "Encountered an exception: %s. Gracefully terminate the rapp",
                exc,
            )
            await self.cleanup()

    def get_registry_href(self):
        """
        Bootstrap the rApp to get the service registry href.
        If the service href is not returned then raise an exception.
        """
        self.service_map, err = rapp.bootstrap(
            self.custom_headers, self.metrics_counters
        )
        if err is not None:
            raise rapp.RappError("Failed to bootstrap: %s" % err)
        if "service" not in self.service_map:
            raise rapp.RappError("Service registry href not found")

    async def register_with_service_registry(self) -> int:
        """
        Register the rApp with the SME service registry.

        Builds the consumer service object from class values and registers
        with the service registry.

        Failure to register is a fatal error.
        """
        service = {
            "name": self.service_name,
            "version": self.service_version,
            "display_name": self.service_display_name,
            "description": self.service_description,
            "service_type": rapp.default_service_type,
            "service_href": self.service_self_url,
        }
        results, err = rapp.register(
            self.service_map["service"],
            service,
            self.custom_headers,
            self.metrics_counters,
        )
        if err is not None:
            raise rapp.RappError("Failed to register service with registry: %s" % err)

        return results["heartbeat_timer"]

    def update_service_map(self):
        """
        Discover and store R1 services.

        Update the local map of discovered services for later use.
        """
        if self.testing_mode:
            self.service_map[
                rapp.platform_service_for_consumer
            ] = self.testing_consumer_href
            return

        platform_services, err = rapp.get_platform_services(
            self.service_map["service"],
            self.custom_headers,
            self.metrics_counters,
        )
        if not platform_services:
            raise rapp.RappError("Failed to retrieve service list: %s" % err)

        # update the service map with platform services
        for name, href in platform_services.items():
            self.service_map[name] = href

    def start_server(self):
        """
        Start the HTTP server on which the rApp will listen for notifications
        from R1 services.

        Set the router for the web app, pass it to the server, and start
        the server.
        """
        fast_api_app = FastAPI()
        fast_api_app.include_router(
            router=server.default_router, prefix=self.service_prefix
        )
        self.server.add_metrics_app(fast_api_app)
        self.server.add_app("", fast_api_app)
        self.server.start()

    def start_data_process(self):
        """
        Start the data process.

        The data process listens on a queue for job result data and other
        messages to do the main processing and analysis of the rApp.
        """
        self.data_process = multiprocessing.Process(
            target=self.data_handler_process_exec
        )
        self.data_process.start()

    async def start_heartbeat(self, heartbeat_timer: int):
        """
        Start periodic heartbeats.

        Heartbears must be regularly sent to the SME to indicate this rApp is
        alive and available.
        """
        self.heartbeat_manager = heartbeat.HeartbeatManager(
            periodicity=heartbeat_timer,
            service=self,
            href=self.service_map["service"],
            service_log=self.log,
            metrics_counters=self.metrics_counters,
        )
        # Set the default heartbeat patch data
        self.heartbeat_manager.set_patch_data()
        await self.heartbeat_manager.start()

    async def can_start_a_new_job(self):
        return self.job_frequency and (
            self.pm_cycles_for_jobs < 0
            or (
                self.pm_cycles_for_jobs >= 0
                and self.pm_cycles_done < self.pm_cycles_for_jobs
            )
        )

    async def start_job_ticker(self):
        """
        Start the generic periodic job ticker.

        In an endless loop call register_jobs_with_consumer_service according
        to job_frequency.

        Override for custom periodic job handling.

        If a job cannot be registered the rApp exits (and will restart in k8s)
        so that the system can be aware of the problem.

        If self.job_frequency is 0, do not register jobs.

        Stop job registrations if self.pm_cycles_done reaches
        self.pm_cycles_for_jobs in case the latter is greater or equal to 0.
        """
        while await self.can_start_a_new_job():
            try:
                # pm_cycles_done increased regardless whether the job registration
                # will be done or not in register_jobs_with_consumer_service.
                self.pm_cycles_done += 1
                self.log.info("Starting PM cycle %d", self.pm_cycles_done)
                self.register_jobs_with_consumer_service()
            except (rapp.RappError, ValueError, KeyError) as exc:
                self.log.exception("Unable to register with DMS: %s", exc)
                await self.cleanup()
                return
            await asyncio.sleep(self.job_frequency)

        if self.pm_cycles_for_jobs >= 0 and self.stop_rapp_when_job_registrations_stop:
            self.log.info(
                "Maximum number of %d PM cycles is reached and "
                "stopping when that happens is configured. Stopping "
                "the rApp."
            )
            await self.cleanup()

    async def start_cmread_ticker(self):
        """
        Start the cmread periodic job ticker.

        If self.cm_data_job_freq is 0, do not run, otherwise perform cmreads
        to read Config Management values.
        """
        cmread_uri = self.get_cmread_uri()
        while self.cm_data_job_freq:
            self.log.info("cmread timer expired")
            self.cmreads(cmread_uri, self.cmread_direct_handler)
            await asyncio.sleep(self.cm_data_job_freq)

    def get_cmread_uri(self):
        """
        Determine the URI for the cmread_direct data-type.

        If we are in testing mode, return a configured URI. Otherwise ask
        the "consumer" service to provide details of the cmread_direct type.
        Those details will include the URI where cmread actions may be performed,
        directly.
        """
        if self.testing_mode:
            return self.testing_cmread_uri

        try:
            consumer_service_uri = self.service_map[rapp.platform_service_for_consumer]
            cmread_uri, err = rapp.get_cmi_uri(
                consumer_service_uri,
                rapp.cmread_direct_info_type,
                self.custom_headers,
                self.metrics_counters,
            )
            if err:
                self.log.error("Unable to find cmread URI: %s", err)
                raise rapp.RappError("Unable to find cmread URI")
        except KeyError as exc:
            self.log.error(
                "Unable to find cmread URI: %s[%s]", exc.__class__.__name__, exc
            )

        return cmread_uri

    def cmreads(self, cmread_uri, callback):
        """
        Perform cmread_direct requests to get CM data.

        :param cmread_uri: The cmread_direct URI where requests will be made.
        :param callback: The handler method where results of the requests will
                         be processed.

        The requests are driven by the value of self.cmread_defs.
        """
        # TODO: Rename this to make it clear it is cmread direct.
        try:
            # Iterate through CM requests
            # TODO: We may want to move the interior of this loop into
            # rapplib/rapp.py so that the details of the http handling are not
            # here in this class.
            for cell_id, parameter_name in self.cmread_defs:
                data = self.cmread_direct(cmread_uri, cell_id, parameter_name)
                callback(cell_id, data)
            self.log.info("periodic cmread done for all the cells")
        except Exception as exc:
            self.log.exception(
                "Encountered an exception in cmreads: %s[%s]. Continuing.", exc
            )

    def cmread_direct(self, cmread_uri, cell_id, parameter_name):
        """
        Perform one cmread_direct request.

        :param cmread_uri: The cmread_direct URI where the request will be made.
        :param cell_id: The cell global id for which data is being requested.
        :param parameter_name: The name of the parameter being required.
        """
        # Update URI with cell ID and parameter name from job definition
        cm_uri = cmread_uri.replace("{cell_id}", str(cell_id)).replace(
            "{parameter_name}", parameter_name
        )
        session = rapp.retryable_http(custom_headers=self.custom_headers)
        try:
            response = session.get(cm_uri)
        except requests.exceptions.RequestException as e:
            self.log.error(
                "unable to get cm data for cell %s: %s[%s]",
                cell_id,
                e.__class__.__name__,
                e,
            )
            return None

        if (response.status_code // 100) != 2:
            # Do not log a 404 response here, as it is common.
            if response.status_code != status.HTTP_404_NOT_FOUND:
                self.log.error(
                    "invalid status (%s) received for cm data for cell (%s)",
                    response.status_code,
                    cell_id,
                )
            return None
        return response.json()

    def cmread_direct_handler(self, cell_id, cmread_data):
        """
        Handle the results of a single cmread_direct request.

        Usually overridden.
        """
        self.log.info("Received from cmi: %s, for cell_id: %s", cmread_data, cell_id)

    async def cleanup(self):
        """
        Clean up tasks and shutdown the rApp.

        This is designed to be capable of being called more than once as an
        interruption may happen while tasks are still being created.
        """
        # Log cleaning up twice
        if self.cleaning_up:
            self.log.info("already cleaning up, return")
            return

        self.log.info("cleaning up")
        self.cleaning_up = True

        # Delete subscriptions that are still active.
        for job_id in self.subscription_job_ids:
            rapp.delete_consumer_job(
                self.service_map[rapp.platform_service_for_consumer],
                job_id,
                self.custom_headers,
            )

        if self.use_data_process and self.data_process:
            if self.data_process.is_alive():
                # first the stop_event is set, so data process queue
                # can terminate gracefully in the next loop
                self.stop_event.set()
                # and then queue_shutdown is added to the queue in
                # to make sure it unblocks in case it's waiting for
                # an item.
                self.data_process_q.put((queue_shutdown, None))
                self.data_process.join()

            # we prefer to cancel the reverse queue task instead of
            # waiting for it to handle any remaining items in order
            # to handle any problematic case in an rapp that would
            # let the shutdown waiting for this queue to finish
            # processing.
            self._reverse_queue_ticker_task.cancel()
            try:
                await self._reverse_queue_ticker_task
            except asyncio.CancelledError:
                # note that the CancelledError exception in reverse
                # queue task needs to be raised again to the creator
                # of it, so we can achieve graceful shutdown of the
                # task.
                self.log.info("reverse queue task now cancelled")

        if self.heartbeat_manager:
            await self.heartbeat_manager.stop()

        if self.server:
            await self.server.stop()

        if self._cmread_ticker_task:
            self._cmread_ticker_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cmread_ticker_task
                self.log.info("cm reader ticker now cancelled")

        if self._job_ticker_task:
            self._job_ticker_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._job_ticker_task
                self.log.info("job ticker now cancelled")

        self.log.info("###### Exiting. Press Ctrl-C again if stalled. #####")

    async def _signal_handler(self, signame):
        """
        Signal received, gracefully shutdown the rApp.
        """
        pid = os.getpid()
        self.log.info(f"Received signal {signame} in {pid}")
        await self.cleanup()

    def register_jobs_with_consumer_service(self):
        """
        Register automated data-request jobs with consumer service.

        Override this method to register custom jobs with the consumer service.

        If pmhistory_counter_names or pmhistory_counter_names and
        pmhistory_cell_ids is set we'll launch a job for those counters,
        automatically. That is, counter names needs to be set.

        update_service_map must be called at least once before this or the
        consumer service will not be found.
        """
        # TODO: This method is supposed to handle multiple data-types, not
        # just pmhistory.
        if self.pmhistory_counter_names:
            self.register_pmhistory_job(
                counter_names=self.pmhistory_counter_names,
                cell_ids=self.pmhistory_cell_ids,
            )

    def register_initial_jobs_with_consumer_service(self):
        """Register a first batch of jobs with the consumer service.

        This runs a first batch of jobs, currently only pmhistory, for a
        defined time window. This allows an rapp to make an initial request for
        a bunch of data, and then switch over to periodic jobs (if job_frequency
        is non-zero).
        """
        # Create job to read topology
        self.register_topology_service_job()
        # TODO: Make this more general.
        pmhistory_initial_gather = self.initial_data_gather[rapp.pm_history_info_type]
        if pmhistory_initial_gather.start != 0 and pmhistory_initial_gather.end != 0:
            self.register_pmhistory_job(
                start_ms=pmhistory_initial_gather.start,
                end_ms=pmhistory_initial_gather.end,
                counter_names=self.pmhistory_counter_names,
                cell_ids=self.pmhistory_cell_ids,
            )

    async def reverse_queue_ticker(self):
        """
        Read from the reverse_data_process_q in the main process.

        The reverse_data_process_q allows the second process to send events and
        data to the main process to change the behavior of the main process.

        For example if data-analysis in the second process concludes that a
        different set of PM counters needs to be retrieved, an event encompassing
        that need can be written to the reverse queue. The second process will
        continue with data-processing while the first process takes care of the
        network requests required to change the PM counter retrieval.

        "Events" on the queue have different types:

        * reverse_handler: A function in the first process is chosen based on
          a data-type attribute included in the event.

        * queue_shutdown: The queue has been told to shutdown, so will exit
          the endless loop, causing the secondary process to exit.

        When the reverse_handler event type is used, a handler method is
        looked up using the get_reverse_handler method. The class level
        dict, data_type_reverse_handler_map, is used to associate the data-type
        in the event with a handler method. If necessary, rApps may choose to
        customise this map to support additional data-types or other methods.
        The "data-type" field can be an R1 data-type, such as "pmhistory", or a
        string unique to the rApp being developed.

        Handlers started from this loop that do network or file input/output,
        or will be long-running should start an asyncio Task to avoid blocking
        the rest of the process.
        """
        self.log.info("Started the reverse queue async task")
        loop = asyncio.get_event_loop()
        while True:
            try:
                # Note: run_in_executor approach doesn't scale to a large
                # number of concurrent tasks: each blocking call "turned async"
                # will take a slot in the thread pool, and those that exceed
                # the pool's maximum number of workers will not even start
                # executing before a thread frees up. For a small number of
                # child processes, using run_in_executor can solve the problem
                # effectively.
                notification_type, f = await loop.run_in_executor(
                    None, self.reverse_data_process_q.get, True, 5
                )
                if notification_type == Reverse_Queue.reverse_handler.value:
                    data_type, data = f()

                    # This will raise an exception if we are not configured
                    # for the data_type. This will crash the service, which is
                    # intended. It is a programming error to not have a handler
                    # configured.
                    handler = self.get_reverse_handler(data_type)
                    handler(data_type, data)
                elif notification_type == Reverse_Queue.queue_shutdown.value:
                    break
                else:
                    raise ValueError(
                        f"unknown reverse queue notification type {notification_type}"
                    )
            except queue.Empty:
                # Encountered empty queue. Will ignore and continue
                continue
            except asyncio.CancelledError:
                self.log.info("Encountered CancelledError for reverse queue task")
                raise

            except Exception as exc:
                self.log.info(
                    "Encountered an exception %s[%s] in reverse queue."
                    " Shutting down gracefully",
                    exc.__class__.__name__,
                    exc,
                )
                raise

    def get_reverse_handler(self, data_type):
        """
        Determine a matching reverse handler for this data_type.or

        Raise KeyError or AttributeError if none found.
        """
        attr_name = self.data_type_reverse_handler_map[data_type]
        return getattr(self, attr_name)

    def cmread_direct_reverse_handler(self, data_type, data):
        """
        Receive reverse handler request for the cmread_direct data-type.

        Usually overridden to perform a direct read request for the defined data.
        """
        self.log.info("see cmread request data %s", data)

    def cmwrite_direct_reverse_handler(self, data_type, data):
        """
        Receive reverse handler request for the cmwrite_direct data-type.

        Usually overridden to perform a direct write request for the defined data.
        """
        self.log.info("see cmwrite direct request data %s", data)

    def cmwrite_reverse_handler(self, data_type, data):
        """
        Receive reverse handler request for the cmwrite data-type

        Usually overridden to launch a cmwrite job-request, using the defined
        data.
        """
        self.log.info("see cmwrite request data %s", data)

    def cmread_reverse_handler(self, data_type, data):
        """
        Receive reverse handler request for the cmread data-type

        Usually overridden to launch a cmread job-request, using the defined
        data.
        """
        self.log.info("see cmread request data %s", data)

    # The methods (not properties) above this line should only happen in the
    # main process. Those that follow, up to data_handler_process_exec, can be
    # used in both. Care should be taken, however, when launching jobs, making
    # any kind of HTTP (or other external) request, any sort of work in a long
    # loop as it will block the processing of the loop in
    # data_handler_process_exec.

    def register_pmhistory_job(
        self,
        start_ms=None,
        end_ms=None,
        start_window_mins=None,
        counter_names=None,
        cell_ids=None,
        technology="nr",
    ):
        """
        Create a pmhistory job and send to DME.

        If it properly registers, return the job id.

        If counter_names is None, that is an error as we don't allow that kind
        of query yet.

        The end of the time window of the query is either that which is set
        by end_ms or the current time.

        The start of the time window of the query is either that which is set
        by:

        * start_ms
        * if start_window_mins is set, a time calculated by taking
          start_window_mins away from the provided or calculated end_ms
        * a time calculated by taking self.pm_data_window_start away from the
          provided or calculated end_ms

        The "window" concept is provided to make it relatively straightforward
        to make a repeating query for the most recent data for the same cells
        and counter names. See the register_jobs_with_consumer_service method,
        above.

        :param start_ms: The start time of the query, in milliseconds. If not
                         set then start_window_mins or its default
                         (self.pm_data_window_start) will be used. Both may not
                         be set.
        :param end_ms: The end time of the query, in milliseconds. If not set
                       the current time is used.
        :param start_window_mins: The number of minutes, prior to end_ms,
                                  to calculate the start of the query. If not
                                  set, self.pm_data_window_start is used.
        :param counter_names: A list of counter names to be queried for.
        :param cell_ids: A list of cell ids to be queried for.
        :param techonlogy: The technology, "nr" or "lte", of the query.
                           Default is "nr".
        """
        if not counter_names:
            raise rapp.RappError("counter_names must be set")

        if start_ms and start_window_mins:
            raise rapp.RappError("only one of start_ms or start_window_mins may be set")

        counter_names = counter_names or []
        cell_ids = cell_ids or []

        # When end_ms is not set, then set it to _now_ in milliseconds since
        # the Unix epoch.
        if not end_ms:
            end_ms = int(time.time() * 1000)

        # When start_ms is not set, then set it to whatever the start window
        # wants it to be.
        if not start_ms:
            if not start_window_mins:
                start_window_mins = self.pm_data_window_start
            start_ms = end_ms - (start_window_mins * 60 * 1000)

        pm_history_job_def = rapp.build_pm_history_job_definition(
            start_ms,
            end_ms,
            counter_names,
            cell_ids=cell_ids,
            technology=technology,
        )

        return self.register_job(
            info_type=rapp.pm_history_info_type,
            job_definition=pm_history_job_def,
        )

    def register_cmwrite_job(self, cm_data_value_list=None, mo_type=None):
        """
        Create a cmwrite job and register with DME.

        If it properly registers, return the job id.

        :param cm_data_value_list: List of CM data in format
                                   {cell_id, parameter_name, parameter_value}.
        """

        if not cm_data_value_list:
            raise rapp.RappError("cm data list is required to register a job")

        cm_data = {
            "values": cm_data_value_list,
        }

        if mo_type:
            cm_data["mo_type"] = mo_type

        self.log.info("sending cmwrite job with %d values", len(cm_data_value_list))

        return self.register_job(
            info_type=rapp.cmwrite_info_type, job_definition=cm_data
        )

    def register_topology_service_job(self):
        """
        Register a job to receive topology information and its updates.
        """
        if self.topologyservice_defs:
            self.log.info("Registering topology service job")
            # Save job id so that job can be deleted when rapp exit.
            topologyservice_job_id = self.register_job(
                rapp.topologyservice_info_type, self.topologyservice_defs
            )
            self.subscription_job_ids.append(topologyservice_job_id)

    def register_cmread_job(
        self,
        cm_multi_get_data=None,
    ):
        """
        Create a cmread job and register with DME.

        If it properly registers, return the job id.

        :param cm_multi_get_data: CmMultiGet in format
                                  {cell_ids, parameter_names}.
        """
        if not cm_multi_get_data:
            raise rapp.RappError("cm data is required to register a job")

        return self.register_job(
            info_type=rapp.cmread_info_type, job_definition=cm_multi_get_data
        )

    def register_cmtrack_job(
        self,
        cmtrack_param=None,
    ):
        """
        Create a cmTrack job and register with DMS.
        If it properly registers, save the cmtrack job id
        :param cmtrack_param: cmTrack params in format
                                  {cell_ids, parameter_names}.
        """
        if not cmtrack_param:
            raise rapp.RappError("cmTrack params is required to register a job")

        # If a job has been previously registered,
        # we need to delete that first as it is a continuous subscription datatype
        if self.cmtrack_job_id:
            rapp.delete_consumer_job(
                self.service_map[rapp.platform_service_for_consumer],
                self.cmtrack_job_id,
                self.custom_headers,
            )
            # Remove the job id from delete during cleanup, we just removed it.
            self.subscription_job_ids.remove(self.cmtrack_job_id)
        job_id = self.register_job(
            info_type=rapp.cmtrack_info_type, job_definition=cmtrack_param
        )
        self.cmtrack_job_id = job_id
        # Keep track of the job id for delete during cleanup.
        self.subscription_job_ids.append(self.cmtrack_job_id)
        return job_id

    def register_job(self, info_type=None, job_definition=None):
        """
        Register a job of any type with the DME.

        Return the job if registration is successful.

        :param info_type: The name of the data or info type of the job (e.g.,
                          "pmhistory", "cmwrite", etc).
        :param job_definition: The job definition, a dict, which specifies what
                               the job will do.
        """
        if not info_type or not job_definition:
            raise rapp.RappError(
                "an info type and job definition is required to register a job"
            )

        # Validate info_type first
        # TODO: This should be based on what we query from DME, not a static
        # list!
        if info_type not in JOB_DATA_TYPES:
            raise rapp.RappError("Invalid info type %s" % info_type)

        # Build job registration info
        job_id = self.new_job_id()
        job_registration = {
            "info_type_id": info_type,
            "job_definition": job_definition,
            "job_owner": self.service_name,
            "job_result_uri": self.job_result_callback(job_id),
            "status_notification_uri": self.job_status_callback(job_id),
        }

        consumer_href = self.service_map[rapp.platform_service_for_consumer]

        _, err = rapp.announce_consumer_job(
            consumer_href,
            job_id,
            job_registration,
            self.custom_headers,
            self.metrics_counters,
        )

        if err is not None:
            # intentionally we don't want to raise an exception and shut down.
            # Based on the rApps we know about, we currently want them to
            # continue registering the next job. But for an rapp which is more
            # concerned about now, it might be important to do something
            # different when it is not possible to register a job. So this
            # might be configurable per rApp in the future.
            self.log.error(
                "failed to register %s job %s with DMS %s", info_type, job_id, err
            )
            return None

        self.log.info("registered %s job %s with DMS", info_type, job_id)
        return job_id

    def new_job_id(self):
        """
        Return a properly formed new job id.
        """
        return str(uuid.uuid4())

    def job_result_callback(self, job_id):
        """
        Return an appropriate results callback URL for the identified job.

        The producer will notify this URL when results are ready for retrieval.
        """
        return (
            self.service_self_url + rapp.consumer_rapp_job_result_url_path_base + job_id
        )

    def job_status_callback(self, job_id):
        """
        Return an appropriate status callback URL for the identified job.

        The DME will notify this URL with status updates about the identified
        job.
        """
        return (
            self.service_self_url + rapp.consumer_rapp_job_status_url_path_base + job_id
        )

    # From this point on these functions execute only in the secondary process.

    def data_handler_process_exec(
        self,
    ) -> None:
        """
        Run the data handler process.

        It is responsible for processing job result data by listening on the
        data_process_q.

        "Events" on the queue have different types:

        * job_result_notification: Result data is available. This is dispatched
          to a "handler" chosen based on the data-type of the job by the
          get_handler func.

        * job_status_notification: The DMS has sent a notification to the rApp
          about the status of a job. This status is sent to the
          job_status_handler.

        * job_result_processed: All the results of a single job have been
          processed. This will call job_processed_hanlder with the data-type.
          This is used to trigger any action which should only happen after a
          job is complete.

        * generic_event: Internal non-job type event has been called. A
          "handler" is chosen based on the event-type by the get_event_handler
          func.

        * queue_shutdown: The queue has been told to shutdown, so will exit
          the endless loop, causing the secondary process to exit.

        rApps that need more complex handling in data process may choose to
        completely override this loop, or override the methods it calls.

        Any processing started from this method (and thus in the second
        process) should avoid spawning off additional threads or using
        asyncio-driven processing as the model assumes single-threaded nature
        for safe data-handling. Any long-running processing will block reads
        from the queue, but the main process will still be able to write to it.

        If processing needs to happen that will block the queue for a very long
        time and data on the queue is expected, rApp authors may wish to spawn
        yet another process. Using multiple processes is the best way to get
        performance gains from Python when doing CPU-bound work.
        """
        self.log.info("Started the data process")

        # We found out that this signal needs to be ignored in order for the
        # "bg" command to work for an rApp job when starting it in foreground.
        signal.signal(signal.SIGTSTP, signal.SIG_IGN)

        while True:
            try:
                if self.stop_event.is_set():
                    self.log.info(
                        "Stop event received. Will gracefully terminate data process"
                    )
                    break

                notification_type, f = self.data_process_q.get(block=True)

                if notification_type == job_result_notification:
                    job_id, data_type, data = f()

                    # This will raise an exception if code has not been written
                    # to handle results for the data_type. This will crash the
                    # service, which is intended. It is a programming error to
                    # not have a handler.
                    handler = self.get_handler(data_type)
                    handler(job_id, data_type, data)
                elif notification_type == job_result_processed:
                    job_id, data_type = f()
                    self.job_processed_handler(job_id, data_type)
                elif notification_type == job_status_notification:
                    job_id, status = f()
                    self.job_status_handler(job_id, status)
                elif notification_type == generic_event:
                    event_type, data = f()

                    # This will raise an exception if we are not configured
                    # for the event_type. This will crash the service, which is
                    # intended. It is a programming error to not have a handler
                    # configured.
                    handler = self.get_handler(event_type)
                    handler(event_type, data)
                elif notification_type == queue_shutdown:
                    self.log.info("Shutting down the data queue")
                    break
            except KeyboardInterrupt:
                self.log.info(
                    "Encountered KeyboardInterrupt exception."
                    " Shutting down the data queue"
                )
                break
            except Exception as exc:
                self.log.exception(
                    "Encountered an exception: %s. Gracefully terminate the rapp",
                    exc,
                )
                break
        # end of while loop

        # Parent gets SIGCHLD
        self.log.info("Exiting data process %s", os.getpid())
        sys.exit(1)

    def get_handler(self, data_type):
        """
        Determine a matching data handler for this data_type.

        Raise KeyError or AttributeError if none found.
        """
        attr_name = self.data_type_handler_map[data_type]
        return getattr(self, attr_name)

    def pmhistory_handler(self, job_id, data_type, data):
        """
        Handle a single row of pmhistory data.

        Usually overridden to store the data for later processing.

        NOTE: Logging each row of data can cause slowness in the rApp.
        """
        self.log.info("seeing pmhistory job %s result %s", job_id, data)

    def cmwrite_handler(self, job_id, data_type, data):
        """
        Handle a cmwrite response.

        Usually overridden.
        """
        self.log.info("see cmwrite job %s result, length %d", job_id, len(data))

    def topologyservice_handler(self, job_id, data_type, data):
        """
        Handle topology information.
        """
        self.log.info("see topologyservice job %s result, length %d", job_id, len(data))

    def cmtrack_handler(self, job_id, data_type, data):
        """
        Handle cmTrack information,
        This method should be overridden in consumer class
        """
        self.log.info("cmTrack job %s result, length %d", job_id, len(data))

    def cmread_handler(self, job_id, data_type, data):
        """
        Handle a cmread response.

        Usually overridden.
        """
        self.log.info("see cmread job %s result, length %d", job_id, len(data))

    def job_processed_handler(self, job_id, data_type):
        """
        Handle the job_result_processed message.

        When a job is done, delete it.

        Override if there are data-types for which jobs should not be deleted.
        """
        # Observe that this job is done.
        self.metrics_counters.increment_job_result_counter(data_type)

        if (
            not self.testing_mode
            and data_type not in CONTINUOUS_SUBSCRIPTION_DATA_TYPES
        ):
            # Data has been received and the job is no longer needed.
            rapp.delete_consumer_job(
                self.service_map[rapp.platform_service_for_consumer],
                job_id,
                self.custom_headers,
            )

    def job_status_handler(self, job_id, status):
        """
        Handle the job_status_notification message.

        We received a job status, if it is DISABLED, delete the job.

        Override if more intelligent handling is desired.
        """
        self.log.info("for job %s status is %s", job_id, status)
        if not self.testing_mode and status == "DISABLED":
            rapp.delete_consumer_job(
                self.service_map[rapp.platform_service_for_consumer],
                job_id,
                self.custom_headers,
            )

    def topologyservice_reverse_handler(self, data_type, data):
        """
        Handle topologyservice job response. Usually overriden.
        """
        self.log.info("topologyservice data %s", data)
