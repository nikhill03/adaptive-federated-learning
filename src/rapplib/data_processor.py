"""
DataProcessor processes data to make observations or changes to the RAN.
It runs in the second process of the rApp. It reads from the processing queue
and writes to management queue.
"""

import os
from typing import Dict
import multiprocessing
import signal
import sys
import copy

from rapplib import rapp, server
from rapplib.server import (
    job_result_notification,
    job_result_processed,
    job_status_notification,
    generic_event,
    queue_shutdown,
)


# All the data types which rApp should keep the job to receive multiple change
# notifications as job results
# TODO:: We should make the value of this variable be filled by
#  querying the subscription data-types that the dms hosts,
#  and looking at a piece of metadata which is available in the data-type definition.
CONTINUOUS_SUBSCRIPTION_DATA_TYPES = [
    rapp.cmtrack_info_type,
    rapp.topologyservice_info_type,
]


class DataProcessor(multiprocessing.Process):
    """
    It's responsible for processing data specific to the rApp in order to
    analyze or make changes to the RAN.

    Handler functions need to be implemented for every job related type or
    generic event type thas is received by the processing queue.

    The job related handlers get a job_id, the data-type and a single data
    item as arguments. What that data item is depends on the content-type of
    the jobs results. If it is "application/json" it will be the entire JSON
    object (decoded from JSON). If it is streaming (line-separated) JSON,
    it will be one row of the response.

    Jobs can be created with the job_request method (below).
    """

    def __init__(self, stop_event=None, log=None, config=None):
        self.log = log
        self.stop_event = stop_event
        self.config = config
        super().__init__()

    def pre_run(self):
        """
        Override this method for activity that should happen after DataProcessor
        forks and before loop handling starts.
        """

    def signal_ready(self):
        """
        Signal that pre_run has completed and we are starting the data processing
        loop. Think hard before choosing to use this!
        """
        if self.config.testing_mode:
            self._send_empty_pmhistory_job()

    def run(self) -> None:
        """
        Run the data handler process.

        It is responsible for processing job result data and other generic type
        of data by listening on the processing_q.

        "Events" on the queue have different types:

        * job_result_notification: Result data is available. This is dispatched
          to a "handler" chosen based on the data-type of the job by the
          __get_handler func.

        * job_status_notification: The DMS has sent a notification to the rApp
          about the status of a job. This status is sent to the
          __job_status_handler.

        * job_result_processed: All the results of a single job have been
          processed. This will call job_processed_handler with the data-type.
          This is used to trigger any action which should only happen after a
          job is complete.

        * generic_event: Internal non-job type result data is available.
          A "handler" is chosen based on the data-type by the
          __get_event_handler func.

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
        self.pre_run()
        self.signal_ready()
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

                notification_type, f = self.config.processing_q.get(True)
                if notification_type == job_result_notification:
                    job_id, data_type, data = f()

                    # This will raise an exception if we are not configured
                    # for the data_type. This will crash the service, which is
                    # intended. It is a programming error to not have a handler
                    # configured.
                    handler = self.__get_handler(data_type)
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
                    # intended. It is a programming error to now have a handler
                    # configured.
                    handler = self.__get_event_handler(event_type)
                    handler(event_type, data)
                elif notification_type == queue_shutdown:
                    self.log.info("Shutting down the data queue")
                    break
            except KeyboardInterrupt as exc:
                self.log.info(
                    "Encountered KeyboardInterrupt exception."
                    " Shutting down the data queue [%s]",
                    exc,
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

    def __get_handler(self, data_type):
        """
        Determine a matching data handler for this data_type.

        Raises AttributeError if no handler has been written.
        """
        attr_name = f"{data_type}_data_handler"
        try:
            return getattr(self, attr_name)
        except AttributeError as exc:
            raise AttributeError(
                "A handler for data-type %s named %s must be defined: %s"
                % (data_type, attr_name, exc)
            ) from exc

    def __get_event_handler(self, event_type):
        """
        Determine a matching event handler for this event.

        Raises AttributeError if no handler has been written.
        """
        attr_name = f"{event_type}_event_handler"
        try:
            return getattr(self, attr_name)
        except AttributeError as exc:
            raise AttributeError(
                "A handler for event-type %s named %s must be defined: %s"
                % (event_type, attr_name, exc)
            ) from exc

    def job_processed_handler(self, job_id, data_type):
        """
        Handle the job_result_processed message.

        When a job is done, delete it.

        Override if there are data-types for which jobs should not be deleted.
        """
        # Observe that this job completed.
        self.config.metrics_counters.increment_job_result_counter(data_type)
        if (
            not self.config.testing_mode
            and data_type not in CONTINUOUS_SUBSCRIPTION_DATA_TYPES
        ):
            # Data has been received and the job is no longer needed.
            self.delete_job(job_id)

    def job_status_handler(self, job_id, status):
        """
        Handle the job_status_notification message.

        We received a job status, if it is DISABLED, delete the job.

        Override if more intelligent handling is desired.
        """
        self.log.info("for job %s status is %s", job_id, status)
        if not self.config.testing_mode and status == "DISABLED":
            self.delete_job(job_id)

    def delete_job(self, job_id):
        """
        Sends a delete job event to DataManager, which will then call
        rapp.delete_consumer_job so it's the only class that has service_map
        as a variable member.
        @param job_id is the job id to be deleted.
        """
        self.send_management_event("delete_job", job_id)

    def send_management_event(self, event_type, data):
        """
        Puts a generic event type to management queue, which is handled by
        the main process.
        @param event_type the type of the event as string.
        @param data the data that will be put in the management queue.
        """
        self.config.management_q.put(
            (
                server.generic_event,
                server.make_partial(
                    server.generic_queue_request,
                    event_type,
                    copy.copy(data),
                ),
            )
        )

    def job_request(self, job_description: Dict[str, str]):
        """
        Puts an job related event type to management queue, which is handled
        by the main process.
        @param job_description the definition data to put into the management
               queue. It's a dictionary of "data_type" and "job_defintion".
        """
        self.send_management_event("job_request", job_description)

    def _send_empty_pmhistory_job(self):
        """
        In testing mode send a meaningless pmhistory job so testing frameworks
        know to send jobs.

        Note that this job intentionally sends a bad counter name.
        """
        job_description = {
            "data_type": rapp.pm_history_info_type,
            "job_definition": {
                "counter_names": ["not-there"],
            },
        }
        self.job_request(job_description)
