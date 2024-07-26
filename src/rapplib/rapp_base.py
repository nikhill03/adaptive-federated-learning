"""
RAppBase provides the base class for an rApp. Implementing rApps may subclass
RAppBase and override functionality as required.
"""

import os
import multiprocessing
from typing import NamedTuple


from rapplib import (
    data_manager,
    data_processor,
    server,
    metrics,
)

# Constants

# seconds
JOB_FREQUENCY_DEFAULT = 600
# minutes
PM_DATA_WINDOW_START_DEFAULT = 15
VMWARE = "VMware"
# seconds
CM_REQUEST_FREQUENCY_DEFAULT = "900"
# by default, pmhistory job registrations will run for ever
PM_CYCLES_FOR_JOBS_DEFAULT = 0
# disabled by default
STOP_RAPP_WHEN_JOB_REGISTRATIONS_STOP_DEFAULT = "false"
# Indicates for how long to ask for pmhistory data. After that time, the rApp
# switches to pmlive data.
PMHISTORY_DURATION_SEC_DEFAULT = "300"
# If it's true, pmlive is requested after pmhistory_duration_sec_default is
# passed.
PMLIVE_DATA_FETCHING_DEFAULT = "false"
# seconds
PMLIVE_DATA_PROCESS_FREQ_DEFAULT = "900"

# if it's true, RTS will be used to get the cell_info
USE_TOPOLOGY_SERVICE_DEFAULT = "false"
# Management Job Handler Q
management_job_handler_q: multiprocessing.Queue = multiprocessing.Queue()


class DataWindow(NamedTuple):
    """A tuple indicating a start and end time window for job."""

    start: int
    end: int


class RAppBase:
    """
    RAppBase is a base class which can be used to manage the configuration
    and creation of an rApp.

    A consumer rApp may subclass RAppBase and override methods and class
    variables to achieve desired functionality.

    Mainly, the data_processor_class and data_manager_class will need to be
    overridden in order for the rApp to define its own data and event handlers.

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
    * data_processor_class: It runs in the second process and handles data
                            from the processing queue. It writes to the
                            management queue.
    * data_management_class: It runs in the main process and handles data from
                             the management queue. It writes to the processing
                             queue.

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

    testing_mode = False
    testing_cmread_uri = ""
    testing_consumer_href = ""

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

    # If cmread_defs has length, then DataManager will do cm read for
    # these jobs definitions every CM_REQUEST_FREQUENCY seconds.
    # Format is [{cell_id1, parameter_name1}, {cell_id2, parameter_name2}, ...]
    cmread_defs = []

    # topologyservice job will be registered if topologyservice_defs is not empty.
    # A topologyservice consumer will receive notification when an update
    # to topology information is available. Notifications are received until
    # job is deleted.
    topologyservice_defs = {}

    # data_manager_class names the class that does data retrieval and other
    # input output handling. It runs in the main process.
    # If custom data management behavior is required, the DataManager class
    # may be subclassed. If a subclass is used, the name of the class must be
    # set here.
    data_manager_class = data_manager.DataManager

    # data_process_class names the class that runs in the second process,
    # so that CPU intensive processing does not block the input/output
    # processing that happens in the first process.
    # If custom data processing behavior is required, the DataProcessor class
    # may be subclassed. If a subclass is used, the name of the class must be
    # set here.
    data_processor_class = data_processor.DataProcessor

    def __init__(self, logger, self_url=None):
        """Initializes but does not start a RAppBase.

        :param logger: A logging.Logger created in the caller. Required.
        :param self_url: If provided, the HTTP URL where this rApp is listening.
                         If not set the value of the SELF_URL environment variable
                         will be used. If that is not set the service name and
                         port will be used to generate a value. In most situations
                         SELF_URL should be set in the environment.
        """
        self.log = logger

        if self_url is not None:
            self.service_self_url = self_url + self.service_prefix
        else:
            self.service_self_url = (
                os.environ.get("SELF_URL", self.__self_url_default)
                + self.service_prefix
            )
        # Override job_frequency if JOB_FREQUENCY is set.
        if os.environ.get("JOB_FREQUENCY"):
            self.job_frequency = int(os.environ.get("JOB_FREQUENCY"))
        self.pmhistory_duration_sec = int(
            os.environ.get("PMHISTORY_DURATION_SEC", PMHISTORY_DURATION_SEC_DEFAULT)
        )
        self.pmlive_data_fetching = (
            os.environ.get("PMLIVE_DATA_FETCHING", PMLIVE_DATA_FETCHING_DEFAULT).lower()
            == "true"
        )
        self.pmlive_data_process_freq = int(
            os.environ.get("PMLIVE_DATA_PROCESS_FREQ", PMLIVE_DATA_PROCESS_FREQ_DEFAULT)
        )
        if os.environ.get("WINDOW_SIZE"):
            self.pm_data_window_start = int(os.environ.get("WINDOW_SIZE"))

        self.cm_data_job_freq = int(
            os.environ.get("CM_REQUEST_FREQUENCY", CM_REQUEST_FREQUENCY_DEFAULT)
        )
        self.use_topology_service = (
            os.environ.get("USE_TOPOLOGY_SERVICE", USE_TOPOLOGY_SERVICE_DEFAULT).lower()
            == "true"
        )

        # timeshift_ms is the number of milliseconds by which to shift the
        # start and end time of a pmhistory job (or other periodic job in the
        # future), only when doing periodic jos handled by self.job_frequency.
        # We default to None here so that it is possible to call the underlying
        # method with a meaningful integer 0. PERIODIC_JOB_TIMESHIFT is a
        # positive integer number of seconds into the past. So if we wanted
        # periodic jobs to start hours before now we could set the environment
        # variable to "86400".
        self.timeshift_ms = (
            int(os.environ.get("PERIODIC_JOB_TIMESHIFT", "0")) * 1000
        ) or None

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
        if self.pm_cycles_for_jobs > 0:
            self.log.info(
                "Job registration will happen for %d PM cycles", self.pm_cycles_for_jobs
            )
            if self.stop_rapp_when_job_registrations_stop:
                self.log.info(
                    "rApp will stop as soon as the configured %d number"
                    " of PM cycles is reached",
                    self.pm_cycles_for_jobs,
                )
        else:
            self.log.info("Job registration won't stop")

        self.stop_event = multiprocessing.Event()

        # Data for second process is provided over the processing_q
        # which is read in an endless loop in data_handler_process_exec
        self.processing_q: multiprocessing.Queue = server.job_handler_q

        # The queue provides a mechanism whereby data_processor can cause
        # additional data gathering actions (I/O) in the main (async) process.
        self.management_q: multiprocessing.Queue = management_job_handler_q

        # DataProcessor and DataManager need to be initialised after the
        # queues have been instantiated.
        self.data_processor = self.data_processor_class(
            log=self.log,
            stop_event=self.stop_event,
            config=self,
        )
        self.data_manager = self.data_manager_class(log=self.log, config=self)

        # The default prometheus metrics the rApp will produce, keeping track
        # of service registration and HTTP errors. An rApp may also choose to
        # add any additional prometheus metrics in its own __init__.
        self.metrics_counters = metrics.MetricsCounter(self.service_name)
        self.custom_headers = {"user-agent": self.__service_user_agent}

    @property
    def __self_url_default(self):
        """
        The default url of this service, created from the service_name and
        service_port. This may be overridden by setting SELF_URL in the
        environment.
        """
        return "http://" + self.service_name + ":" + self.service_port

    @property
    def __service_user_agent(self):
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
            "source_id": self.service_name,
            "source_vendor": self.vendor,
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
            "source_id": self.service_name,
            "source_vendor": self.vendor,
        }

    async def register_with_service_registry(self) -> int:
        """
        Register the rApp with the SME service registry.

        Builds the consumer service object from class values and registers
        with the service registry.

        It's needed in RappBase also because it's called by HeartbeatManager.
        """
        return await self.data_manager.register_with_service_registry()

    async def cleanup(self):
        """
        Clean up tasks and shutdown the rApp.

        This is designed to be capable of being called more than once as an
        interruption may happen while tasks are still being created.

        It's needed in RappBase also because it's called by HeartbeatManager.
        """
        await self.data_manager.cleanup()

    async def new_rapp_session(self):
        """
        The data manager will start the web server, do service and job
        registrations and start the second process (as the DataProcessor).
        """
        await self.data_manager.new_rapp_session()
