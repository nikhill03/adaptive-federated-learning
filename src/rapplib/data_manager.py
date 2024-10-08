"""
DataManager is the class that does data retrieval and other input output
handling. It runs in the first process of the rApp reading from the
management queue and writing to the processing queue.
"""

import os
import asyncio
from contextlib import suppress
import signal
import sys
import time
import queue
import uuid
import copy

from fastapi import FastAPI, status

from rapplib import (
    rapp_async_session,
    rapp,
    rapp_async,
    server,
    heartbeat,
    asyncio_logger,
)


# Constants
CMREAD_DIRECT_HANDLER = "cmread_direct"

JOB_DATA_TYPES = [
    rapp.pm_history_info_type,
    rapp.pm_live_info_type,
    rapp.cmwrite_info_type,
    rapp.topologyservice_info_type,
    rapp.cmread_info_type,
    rapp.cmtrack_info_type,
]


class DataManager:
    """
    It's the class that handles I/O data. It gets data from the management
    queue of the main process and writes to the processing queue of the second
    process. It starts the web server and any asyncio tasks. It takes care of
    service and job registrations. It also starts the data processing process.
    """

    # subscription_job_ids is a list of active subscriptions which should be
    # deleted at shutdown time
    subscription_job_ids = []

    # cmtrack job id needs to be saved,
    # so it can be cleared when a new job request is sent
    cmtrack_job_id = ""

    def __init__(self, log=None, config=None):
        self.log = log
        self.config = config
        self.cleaning_up = False
        self._exiting = False

        # A map of discoverd services and their URLs
        self.service_map = {}

        # The job definitions for pmlive data type
        self.pmlive_job_defs = []

        # The heartbeat_manager automatically sends liveness messages to the
        # service registry at intervals determined by the registry.
        self.heartbeat_manager: heartbeat.HeartbeatManager = None

        # The web server that an rApp must run to receive job notifications.
        self.server = server.Server(
            service_url=self.config.service_self_url, service_log=self.log
        )

        # Keep a reference of the job ticker as that is what keeps the
        # application running and allows it to be shut down easily (by
        # cancelling the task).
        self._job_ticker_task: asyncio.Future = None

        # Keep a reference of the cmread ticker
        self._cmread_ticker_task: asyncio.Future = None

        # count how many pm cycles job registrations happen for, so it can
        # be compared with the configurable self.config.pm_cycles_for_jobs.
        self.pm_cycles_done = 0

        # The management_queue_ticket_task is an asyncio task started by this
        # class.
        self._management_queue_ticker_task: asyncio.Future = None

        # The pm_data_process_task is an asyncio task started by this
        # class.
        self._pm_data_process_task: asyncio.Future = None

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
            # Do not log as exception here because if this is a memory error,
            # there will be a fatal error (sooner than we want).
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

        if not self.config.testing_mode:
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
        self.start_data_processor()

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

        # 6. Start the management queue job for getting messages from the data
        # process.
        self._management_queue_ticker_task = asyncio_logger.create_task(
            self.management_queue_ticker(),
            logger=self.log,
            message="Task raised an exception",
        )

        # 7. Setup the web server and app, and start it.
        self.start_server()
        # Do a brief sleep to yield back to the server so that it finishes
        # starting up. If we do not jobs will often be registered prior
        # to the server being ready.
        await asyncio.sleep(1)

        if not self.config.testing_mode:
            # 8. Start the liveness heartbeat.
            await self.start_heartbeat(heartbeat_timer)

            # 9. Register initial jobs.
            if not await self.register_initial_jobs_with_consumer_service():
                return

        # 10. Start periodic CM read request tickers
        try:
            self._cmread_ticker_task = asyncio_logger.create_task(
                self.start_cmread_direct_ticker(),
                logger=self.log,
                message="Task raised an exception",
            )
        except (rapp.RappError, ValueError, KeyError) as exc:
            self.log.exception("Unable to start cmread ticker: %s", exc)
            await self.cleanup()
            return

        if not self.config.testing_mode:
            # 11. Start the periodic job request ticker and await its completion.
            self._job_ticker_task = asyncio_logger.create_task(
                self.start_job_ticker(),
                logger=self.log,
                message="Task raised an exception",
            )

    async def start_pmlive_data_process_ticker(self):
        """
        For pmlive mode, in an endless loop, process the stored
        PM data according to pmlive_data_process_freq.
        """
        self.log.info("Started pmlive data process ticker")
        while True:
            await asyncio.sleep(self.config.pmlive_data_process_freq)
            self.log.info("pmlive data process ticker expired")
            self.send_generic_event("pm_data_process", None)

    async def await_tasks(self):
        """Await all tasks to allow the rApp to cleanly shutdown."""
        tasks = [
            task for task in asyncio.all_tasks() if task is not asyncio.current_task()
        ]

        try:
            await asyncio.gather(*tasks)

        except asyncio.CancelledError as exc:
            self.log.info(
                "all tasks now cancelled, exception %s[%s]",
                exc.__class__.__name__,
                exc,
            )
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
            self.config.custom_headers, self.config.metrics_counters
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
            "name": self.config.service_name,
            "version": self.config.service_version,
            "display_name": self.config.service_display_name,
            "description": self.config.service_description,
            "service_type": rapp.default_service_type,
            "service_href": self.config.service_self_url,
        }
        results, err = await rapp_async.register(
            self.service_map["service"],
            service,
            self.config.custom_headers,
            self.config.metrics_counters,
        )
        if err is not None:
            raise rapp.RappError("Failed to register service with registry: %s" % err)

        return results["heartbeat_timer"]

    def update_service_map(self):
        """
        Discover and store R1 services.

        Update the local map of discovered services for later use.
        """
        if self.config.testing_mode:
            self.service_map[
                rapp.platform_service_for_consumer
            ] = self.config.testing_consumer_href
            return

        platform_services, err = rapp.get_platform_services(
            self.service_map["service"],
            self.config.custom_headers,
            self.config.metrics_counters,
        )
        if not platform_services:
            raise rapp.RappError("Failed to retrieve service list: %s" % err)

        # update the service map with platform services
        for name, href in platform_services.items():
            self.service_map[name] = href
        
        self.log.debug(f'Service URLs: {self.service_map}')

    def start_server(self):
        """
        Start the HTTP server on which the rApp will listen for notifications
        from R1 services.

        Set the router for the web app, pass it to the server, and start
        the server.
        """
        fast_api_app = FastAPI()
        fast_api_app.include_router(
            router=server.default_router, prefix=self.config.service_prefix
        )
        self.server.add_metrics_app(fast_api_app)
        self.server.add_app("", fast_api_app)
        self.server.start()

    def start_data_processor(self):
        """
        Start the data process.

        The data process listens on a queue for job result data and other
        messages to do the main processing and analysis of the rApp.
        """
        self.config.data_processor.start()

    async def start_heartbeat(self, heartbeat_timer: int):
        """
        Start periodic heartbeats.

        Heartbears must be regularly sent to the SME to indicate this rApp is
        alive and available.
        """
        # TODO This interface to HeartbeatManager is quite confusing as we're
        # passing the class as service which doesn't quite make sense.
        self.heartbeat_manager = heartbeat.HeartbeatManager(
            periodicity=heartbeat_timer,
            service=self.config,
            href=self.service_map["service"],
            service_log=self.log,
            metrics_counters=self.config.metrics_counters,
        )
        # Set the default heartbeat patch data
        self.heartbeat_manager.set_patch_data()
        await self.heartbeat_manager.start()

    async def can_start_a_new_job(self):
        return self.config.job_frequency and (
            self.config.pm_cycles_for_jobs <= 0
            or (
                self.config.pm_cycles_for_jobs > 0
                and self.pm_cycles_done < self.config.pm_cycles_for_jobs
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
        self.config.pm_cycles_for_jobs in case the latter is greater than 0.
        """
        self.log.info(
            "Starting pm job ticker, with testing %s, pmlive "
            "fetching %s, pmhistory duration %d sec, pmlive "
            "data process frequency %d sec and job frequency %f",
            self.config.testing_mode,
            self.config.pmlive_data_fetching,
            self.config.pmhistory_duration_sec,
            self.config.pmlive_data_process_freq,
            self.config.job_frequency,
        )
        startTime = time.time()

        while await self.can_start_a_new_job():
            try:
                job_sleep = self.config.job_frequency
                if self.config.pmlive_data_fetching:
                    now = time.time()
                    if now - startTime >= self.config.pmhistory_duration_sec:
                        # only get pmhistory results for pmhistory_duration_sec
                        self.log.info(
                            "PM history data fetching completed. "
                            "Will now get pmlive data"
                        )
                        # as soon as getting pm data finishes, make request for
                        # pmlive data. This is not an async tasks so there's
                        # nothing to await!
                        await self.register_pmlive_jobs_with_consumer_service()
                        # Now the pmlive data processing timer can start.
                        if self.config.pmlive_data_process_freq > 0:
                            self._pm_data_process_task = asyncio_logger.create_task(
                                self.start_pmlive_data_process_ticker(),
                                logger=self.log,
                                message="Task raised an exception",
                            )
                        return
                    if self.config.job_frequency > self.config.pmhistory_duration_sec:
                        job_sleep = self.config.pmhistory_duration_sec

                # pm_cycles_done increased regardless whether the job registration
                # will be done or not in register_jobs_with_consumer_service.
                self.pm_cycles_done += 1
                self.log.info("Starting PM cycle %d", self.pm_cycles_done)

                await self.register_jobs_with_consumer_service()

                self.log.info(
                    "going to sleep in start_job_ticker for %s seconds", job_sleep
                )
                await asyncio.sleep(job_sleep)
            except (rapp.RappError, ValueError, KeyError) as exc:
                self.log.exception("Unable to register with DMS: %s", exc)
                await self.cleanup()
                return

        if self.config.pm_cycles_for_jobs > 0 and (
            self.config.stop_rapp_when_job_registrations_stop
        ):
            self.log.info(
                "Maximum number of %d PM cycles is reached and "
                "stopping when that happens is configured. Stopping "
                "the rApp."
            )
            await self.cleanup()

    async def start_cmread_direct_ticker(self):
        """
        Start the cmread_direct periodic job ticker.

        If cm_data_job_freq is 0, do not run, otherwise perform
        cmreads_direct to read Config Management values.
        """
        cmread_uri = await self.get_cmread_uri()
        while self.config.cm_data_job_freq:
            self.log.info("cmread direct timer expired")
            if self.config.cmread_defs:
                await self.cmreads_direct(cmread_uri, self.cmread_direct_callback)

            await asyncio.sleep(self.config.cm_data_job_freq)

    async def get_cmread_uri(self):
        """
        Determine the URI for the cmread_direct data-type.

        If we are in testing mode, return a configured URI. Otherwise ask
        the "consumer" service to provide details of the cmread_direct type.
        Those details will include the URI where cmread actions may be performed,
        directly.
        """
        if self.config.testing_mode:
            return self.config.testing_cmread_uri

        try:
            consumer_service_uri = self.service_map[rapp.platform_service_for_consumer]
            cmread_uri, err = await rapp_async.get_cmi_uri(
                consumer_service_uri,
                rapp.cmread_direct_info_type,
                self.config.custom_headers,
                self.config.metrics_counters,
            )
            if err:
                self.log.error("Unable to find cmread URI: %s", err)
                raise rapp.RappError("Unable to find cmread URI")
        except KeyError as exc:
            self.log.error(
                "Unable to find cmread URI: %s[%s]", exc.__class__.__name__, exc
            )

        return cmread_uri

    async def cmreads_direct(self, cmread_uri, callback):
        """
        Perform cmread_direct requests to get CM data.

        :param cmread_uri: The cmread_direct URI where requests will be made.
        :param callback: The handler method where results of the requests will
                         be processed.

        The requests are driven by the value of cmread_defs.
        """
        try:
            # Iterate through CM requests
            for cell_id, parameter_name in self.config.cmread_defs:
                data = await self.cmread_direct(cmread_uri, cell_id, parameter_name)
                callback(cell_id, data)
            self.log.info("periodic cmread done for all the cells")
        except Exception as exc:
            self.log.exception(
                "Encountered an exception: %s. Gracefully terminate the rapp", exc
            )
            await self.cleanup()

    async def cmread_direct(self, cmread_uri, cell_id, parameter_name):
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
        # TODO: We may want to move the interior of this loop into
        # rapplib/rapp.py so that the details of the http handling
        # are not here in this class.
        try:
            json_results, status_code = await rapp_async_session.get_json(
                rapp_async_session.get_session(
                    custom_headers=self.config.custom_headers
                ),
                cm_uri,
                None,
            )
            if (status_code // 100) != 2:
                # Do not log a 404 response here, as it is common.
                if status_code != status.HTTP_404_NOT_FOUND:
                    self.log.error(
                        "invalid status (%s) received for cm data for cell (%s)",
                        status_code,
                        cell_id,
                    )
                return None

            return json_results

        except Exception as e:
            self.log.error(
                "unable to get cm data for cell %s: %s[%s]",
                cell_id,
                e.__class__.__name__,
                e,
            )
            return None

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
                self.config.custom_headers,
            )

        if self.config.data_processor.is_alive():
            # first the stop_event is set, so data process queue
            # can terminate gracefully in the next loop
            self.config.stop_event.set()
            # and then queue_shutdown is added to the queue in
            # to make sure it unblocks in case it's waiting for
            # an item.
            self.send_shutdown_event()
            self.config.data_processor.join()

        # we prefer to cancel the management queue task instead of
        # waiting for it to handle any remaining items in order
        # to handle any problematic case in an rapp that would
        # let the shutdown waiting for this queue to finish
        # processing.
        if self._management_queue_ticker_task:
            self._management_queue_ticker_task.cancel()
            try:
                await self._management_queue_ticker_task
            except asyncio.CancelledError as exc:
                # note that the CancelledError exception in management
                # queue task needs to be raised again to the creator
                # of it, so we can achieve graceful shutdown of the
                # task.
                self.log.info(
                    "management queue task now cancelled %s[%s]",
                    exc.__class__.__name__,
                    exc,
                )

        if self.heartbeat_manager:
            await self.heartbeat_manager.stop()
            self.log.info("heartbeat manager stopped")

        if self.server:
            await self.server.stop()
            self.log.info("server stopped")

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

    async def register_jobs_with_consumer_service(self):
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
        if self.config.pmhistory_counter_names:
            await self.register_pmhistory_job(
                counter_names=self.config.pmhistory_counter_names,
                cell_ids=self.config.pmhistory_cell_ids,
                timeshift_ms=self.config.timeshift_ms,
            )

    async def register_initial_jobs_with_consumer_service(self):
        """Register a first batch of jobs with the consumer service.

        This runs a first batch of jobs, currently only pmhistory, for a
        defined time window. This allows an rapp to make an initial request for
        a bunch of data, and then switch over to periodic jobs (if job_frequency
        is non-zero).
        """
        try:
            # Create job to read topology
            if self.config.use_topology_service:
                await self.register_topology_service_job()
            # TODO: Make this more general.
            pmhistory_initial_gather = self.config.initial_data_gather[
                rapp.pm_history_info_type
            ]
            if (
                pmhistory_initial_gather.start != 0
                and pmhistory_initial_gather.end != 0
            ):
                await self.register_pmhistory_job(
                    start_ms=pmhistory_initial_gather.start,
                    end_ms=pmhistory_initial_gather.end,
                    counter_names=self.config.pmhistory_counter_names,
                    cell_ids=self.config.pmhistory_cell_ids,
                )
        except Exception as exc:
            self.log.exception(
                "Encountered an exception: %s. Gracefully terminate the rapp",
                exc,
            )
            await self.cleanup()
            return False

        return True

    async def management_queue_ticker(self):
        """
        Run the management data handler in the main process. It allows the
        second process to send events and data to the main process to change
        the behavior of the main process.

        Usually an event type is added into the management queue by a handler
        of the second process, then the right handler is identified by the
        __get_event_handler.

        "Events" on the queue have different types:

        * generic_event: The right handler is identified by the
                         __get_event_handler func.

        * queue_shutdown: The queue has been told to shutdown, so will exit the
          the endless loop, causing the secondary process to exit.

        TODO: Handlers started from this loop that do network or file
        input/output, or will be long-running should start an asyncio Task
        to avoid blocking the rest of the process.
        """
        self.log.info("Started the management queue async task")
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
                    None, self.config.management_q.get, True, 5
                )
                if notification_type == server.generic_event:
                    event_type, data = f()

                    # This will raise an exception if we are not configured
                    # for the vent_type. This will crash the service, which is
                    # intended. It is a programming error to not have a handler
                    # configured.
                    handler = self.__get_event_handler(event_type)
                    asyncio_logger.create_task(
                        handler(event_type, data),
                        logger=self.log,
                        message="Handler raised an exception",
                    )
                elif notification_type == server.queue_shutdown:
                    self.log.info("Management queue asked to shutdown")
                    break
                else:
                    raise ValueError(
                        "unknown management queue notification type "
                        f"{notification_type}"
                    )
            except queue.Empty:
                # Encountered empty queue. Will ignore and continue
                continue

            except asyncio.CancelledError as exc:
                self.log.error(
                    "Encountered CancelledError for management queue task %s[%s]",
                    exc.__class__.__name__,
                    exc,
                )
                raise

            except Exception as exc:
                self.log.exception(
                    "Encountered an exception %s in management queue."
                    " Shutting down gracefully",
                    exc,
                )
                raise

    # TODO: this should be awaitable in the future
    def __get_event_handler(self, event_type):
        """
        Determine a matching management handler for this event_type

        Raise AttributeError if none found.
        """
        attr_name = f"{event_type}_event_handler"
        try:
            return getattr(self, attr_name)
        except AttributeError as exc:
            raise AttributeError(
                "A handler for event-type %s named %s must be defined: %s"
                % (event_type, attr_name, exc)
            ) from exc

    async def job_request_event_handler(self, event_type, data):
        """
        This is the handler for the job_request sent by DataProcessor.
        @param event_type will be "job_request"
        @param data It's a dictionary of "data_type" and "job_defintion"
        """
        await self.register_job(data["data_type"], data["job_definition"])

    async def delete_job_event_handler(self, event_type, data):
        job_id = data
        await rapp_async.delete_consumer_job(
            self.service_map[rapp.platform_service_for_consumer],
            job_id,
            self.config.custom_headers,
        )

    def define_pmhistory_job(
        self,
        start_ms=None,
        end_ms=None,
        start_window_mins=None,
        timeshift_ms=None,
        counter_names=None,
        cell_ids=None,
        technology="nr",
    ):
        """Create a pmhistory job definition based on requirements."""
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
                start_window_mins = self.config.pm_data_window_start
            start_ms = end_ms - (start_window_mins * 60 * 1000)

        if timeshift_ms is not None:
            start_ms = start_ms - timeshift_ms
            end_ms = end_ms - timeshift_ms

        return rapp.build_pm_history_job_definition(
            start_ms,
            end_ms,
            counter_names,
            cell_ids=cell_ids,
            technology=technology,
        )

    async def register_pmhistory_job(
        self,
        start_ms=None,
        end_ms=None,
        start_window_mins=None,
        timeshift_ms=None,
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
        * a time calculated by taking pm_data_window_start away from the
          provided or calculated end_ms

        The "window" concept is provided to make it relatively straightforward
        to make a repeating query for the most recent data for the same cells
        and counter names. See the register_jobs_with_consumer_service method,
        above.

        If timeshift_ms is set, then the values calculated for both start_ms
        and end_ms are shifted by that many milliseconds before the calculated
        value. This allows starting periodic jobs in the past.

        :param start_ms: The start time of the query, in milliseconds. If not
                         set then start_window_mins or its default
                         (pm_data_window_start) will be used. Both may not
                         be set.
        :param end_ms: The end time of the query, in milliseconds. If not set
                       the current time is used.
        :param start_window_mins: The number of minutes, prior to end_ms,
                                  to calculate the start of the query. If not
                                  set, pm_data_window_start is used.
        :param timeshift_ms: Number of milliseconds by which to shift both
                             start_ms and end_ms into the past.
        :param counter_names: A list of counter names to be queried for.
        :param cell_ids: A list of cell ids to be queried for.
        :param techonlogy: The technology, "nr" or "lte", of the query.
                           Default is "nr".
        """
        pm_history_job_def = self.define_pmhistory_job(
            start_ms=start_ms,
            end_ms=end_ms,
            start_window_mins=start_window_mins,
            timeshift_ms=timeshift_ms,
            counter_names=counter_names,
            cell_ids=cell_ids,
            technology=technology,
        )

        return await self.register_job(
            info_type=rapp.pm_history_info_type,
            job_definition=pm_history_job_def,
        )

    async def register_cmwrite_job(
        self,
        cm_data_value_list=None,
    ):
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

        self.log.info("sending cmwrite job with %d values", len(cm_data_value_list))

        return await self.register_job(
            info_type=rapp.cmwrite_info_type, job_definition=cm_data
        )

    async def register_topology_service_job(self):
        """
        Register a job to receive topology information and its updates.
        """
        if self.config.topologyservice_defs:
            self.log.info("Registering topology service job")
            # Save job id so that job can be deleted when rapp exit.
            topologyservice_job_id = await self.register_job(
                rapp.topologyservice_info_type, self.config.topologyservice_defs
            )

            if topologyservice_job_id is None:
                raise rapp.RappError(
                    "topologyservice failed to register. rApp exiting."
                )

            self.subscription_job_ids.append(topologyservice_job_id)

    async def register_cmread_job(
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

        return await self.register_job(
            info_type=rapp.cmread_info_type, job_definition=cm_multi_get_data
        )

    async def register_cmtrack_job(
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
            await rapp_async.delete_consumer_job(
                self.service_map[rapp.platform_service_for_consumer],
                self.cmtrack_job_id,
                self.config.custom_headers,
            )
            # Remove the job id from delete during cleanup, we just removed it.
            self.subscription_job_ids.remove(self.cmtrack_job_id)
        job_id = await self.register_job(
            info_type=rapp.cmtrack_info_type, job_definition=cmtrack_param
        )
        self.cmtrack_job_id = job_id
        # Keep track of the job id for delete during cleanup.
        self.subscription_job_ids.append(self.cmtrack_job_id)
        return job_id

    async def register_pmlive_jobs_with_consumer_service(self):
        """
        Registers pmlive jobs with the consumer service.
        """
        for pmlive_job_def in self.pmlive_job_defs:
            await self.register_job(rapp.pm_live_info_type, pmlive_job_def)

    # TODO This needs to be async and be awaitable
    async def register_job(self, info_type=None, job_definition=None):
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
            "job_owner": self.config.service_name,
            "job_result_uri": self.get_job_result_callback_url(job_id),
            "status_notification_uri": self.get_job_status_callback_url(job_id),
        }

        self.log.debug(f'Job registration request data: {job_registration}')

        # Send job registration request to DMS
        consumer_href = self.service_map[rapp.platform_service_for_consumer]
        
        self.log.debug(f'Platform service endpoint for consumer: {consumer_href}')

        _, err = await rapp_async.announce_consumer_job(
            consumer_href,
            job_id,
            job_registration,
            self.config.custom_headers,
            self.config.metrics_counters,
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

    def get_job_result_callback_url(self, job_id):
        """
        Return an appropriate results callback URL for the identified job.

        The producer will notify this URL when results are ready for retrieval.
        """
        return (
            self.config.service_self_url
            + rapp.consumer_rapp_job_result_url_path_base
            + job_id
        )

    def get_job_status_callback_url(self, job_id):
        """
        Return an appropriate status callback URL for the identified job.

        The DME will notify this URL with status updates about the identified
        job.
        """
        return (
            self.config.service_self_url
            + rapp.consumer_rapp_job_status_url_path_base
            + job_id
        )

    def cmread_direct_callback(self, cell_id, cmread_data):
        """
        Handle the results of a single cmread_direct request.

        Usually overridden.
        """
        self.log.info("Received from cmi: %s, for cell_id: %s", cmread_data, cell_id)

    async def cmread_direct_event_handler(self, data_type, data):
        """
        Handle a cmread direct request and send response to data process queue.
        """
        # If there is no data (404) for the cmread, then cmread_data will be
        # None.
        cmread_uri = await self.get_cmread_uri()
        cmread_data = await self.cmread_direct(
            cmread_uri, data["cell_id"], data["parameter_name"]
        )
        if cmread_data is not None:
            self.send_direct_results_notification(CMREAD_DIRECT_HANDLER, cmread_data)

    def send_generic_event(self, event_type, data):
        """
        Puts a generic event type to processing queue, which is handled by
        the second process.
        @param event_type the type of the event as string.
        @param data the data that will be put in the processing queue.
        """
        self.config.processing_q.put(
            (
                server.generic_event,
                server.make_partial(
                    server.generic_queue_request,
                    event_type,
                    copy.copy(data),
                ),
            )
        )

    def send_direct_results_notification(self, event_type, data):
        """
        Puts a direct data-type job result notification to processing queue,
        which is handled by the second process.
        @param event_type the type of the event as string.
        @param data the data that will be put in the processing queue.
        """
        job_id = self.new_job_id()
        self.config.processing_q.put(
            (
                server.job_result_notification,
                server.make_partial(
                    server.job_result_handler,
                    job_id,
                    event_type,
                    copy.copy(data),
                ),
            )
        )

    def send_shutdown_event(self):
        """
        Puts a queue_shutdown event type to processing queue, which is handled by
        the second process.
        """
        self.config.processing_q.put(
            (
                server.queue_shutdown,
                server.make_partial(
                    server.generic_queue_request,
                    server.queue_shutdown,
                    None,
                ),
            )
        )
