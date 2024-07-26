"""
Implements the common heartbeat management code
for all the rapps to use to send periodic heartbeats.
"""

import asyncio
import logging

from fastapi import status
from typing import Dict, List, Any
from contextlib import suppress

from rapplib.rapp import RappError, RegistryException, registry_uri
from rapplib.metrics import MetricsCounter
from rapplib import rapp_async_session


class HeartbeatManager:
    """
    The HeartbeatManager automates the process of sending heartbeats to SME.

    After initial service registration the rApp will receive timing information
    from SME on how often it must PATCH its service record for the SME to accept
    that the rApp is still running and healthy.

    The HeartbeatManager is provided with this timing information. Once
    started it will regularly send heartbeats until told to stop.

    If an attempt to PATCH an existing service record fails because the record
    is gone for some reason, the HeartbeatManager will re-register the service,
    and restart the process of sending PATCHes.
    """

    def __init__(
        self,
        periodicity: int,
        service,
        href: str,
        service_log: logging = None,
        metrics_counters: MetricsCounter = None,
    ):
        self.periodicity: int = periodicity
        self.service = service
        self.href: str = registry_uri(href, service.service_name)
        if service_log is None:
            self.log = logging.getLogger("rapp_service")
        else:
            self.log = service_log
        self.metrics_counters: MetricsCounter = metrics_counters
        self.data: List[Dict[str, Any]] = None
        self.task_started: bool = False
        self._task: asyncio.Future = None

    def set_patch_data(
        self, op: str = "replace", path: str = "/status", value: str = "REGISTERED"
    ) -> None:
        """
        method to set the patch data to be sent periodically foe heartbeat.
        :param op: the json-patch doc operation
        :param path: the json-patch doc path to set
        :param value: the json-patch doc value to be set at above path
        """
        self.data = [{"op": op, "path": path, "value": value}]

    async def execute(self):
        """
        Async coroutine to execute the periodic heartbeats
        """
        while True:
            await asyncio.sleep(self.periodicity)
            # Send the patch request
            try:
                json_results, status_code = await rapp_async_session.patch_json(
                    session=rapp_async_session.get_session(),
                    href=self.href,
                    data=self.data,
                    exception=RegistryException,
                    metrics_counters=self.metrics_counters,
                )
                if status_code != status.HTTP_204_NO_CONTENT:
                    self.periodicity = json_results["heartbeat_timer"]
            except (RegistryException, ValueError, KeyError) as e:
                self.log.error("Error in heartbeat: %s[%s]", e.__class__.__name__, e)
                await self.stop()
                try:
                    self.periodicity = (
                        await self.service.register_with_service_registry()
                    )
                except (RappError, ValueError, KeyError) as exc:
                    self.log.exception("Unable to re-register rapp: %s", exc)
                    await self.service.cleanup()
                    return
                await self.start()

    async def start(self):
        """
        Async coroutine to start the periodic heartbeat execution
        """
        if not self.task_started:
            self.log.info("starting the heartbeat task")
            self.task_started = True
            # Start task to execute heartbeat periodically
            self._task = asyncio.ensure_future(self.execute())

    async def stop(self):
        """
        Async conroutine to stop the periodic heartbeat execution
        """
        if self.task_started:
            self.log.info("stopping the heartbeat task")
            self.task_started = False
            # Stop the periodic heartbeat task
            self._task.cancel()
            try:
                await self._task
            except Exception as exc:
                self.log.info(
                    "Exception %s[%s] raised while heartbeat manager stops. Ignore it.",
                    exc.__class__.__name__,
                    exc,
                )
            self.log.info("stopped the heartbeat task")
