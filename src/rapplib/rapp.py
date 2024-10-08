"""
Provides common code for data producers and consumers
that operate as rApps in the Non-Real-Time RIC environment.
"""

import os
import logging
from typing import Dict, Any, List

from rapplib.metrics import MetricsCounter
from rapplib import rapp_session


# Constant variables
bootstrapper_default = "http://bootstrapper:8080"
consumer_rapp_job_result_url_path_base = "/job_results/"
consumer_rapp_job_status_url_path_base = "/job_statuses/"
default_service_type = "rapp"
platform_service_for_consumer = "consumer"

pm_history_info_type = "pmhistory"
cmwrite_info_type = "cmwrite"
cmread_info_type = "cmread"
cmread_direct_info_type = "cmread_direct"
pm_live_info_type = "pmlive"
topologyservice_info_type = "topologyservice"
cmtrack_info_type = "cmtrack"

ten_pow_twelve = 10**12


class RappException(Exception):
    """
    Base class for errors encountered
    """

    def __init__(self, msg: str, status_code: int = 200, response_text: str = ""):
        if response_text == "":
            super().__init__("failure with " + msg)
        else:
            super().__init__(
                "failure with " + msg + " : %s, %s" % (status_code, response_text)
            )


class BootstrapException(RappException):
    """
    Exception raised for errors encountered with R1 bootstrapper
    """

    def __init__(self, msg: str = "", status_code: int = 200, response_text: str = ""):
        super().__init__("R1 bootstrapper : " + msg, status_code, response_text)


class RegistryException(RappException):
    """
    Exception raised for errors encountered with R1 registry
    """

    def __init__(self, msg: str = "", status_code: int = 200, response_text: str = ""):
        super().__init__("R1 registry : " + msg, status_code, response_text)


class DmsException(RappException):
    """
    Exception raised for error encountered with R1 DMS
    """

    def __init__(self, msg: str = "", status_code: int = 200, response_text: str = ""):
        super().__init__("R1 DMS : " + msg, status_code, response_text)


class JobResultException(RappException):
    """
    Exception raised for errors encountered while fetching job result
    """

    def __init__(self, msg: str = "", status_code: int = 200, response_text: str = ""):
        super().__init__("job result : " + msg, status_code, response_text)


class CmiException(RappException):
    """
    Exception raised for error encountered with R1 CMI
    """

    def __init__(self, msg: str = "", status_code: int = 200, response_text: str = ""):
        super().__init__("R1 CMI : " + msg, status_code, response_text)


class RappError(Exception):
    """An exception raised while executing common rApp operations."""


def configure_default_logging(name: str):
    """
    This should be called by the rapps' main function before the
    rapp object is created. The logger that is created here should
    be passed into the rapp object. All the subsequent times that
    a logger with different name is needed, logging.getLogger
    should be used and the configuration that has happened here
    will apply to those new loggers too.
    """
    logger = logging.getLogger(name=name)
    level = logging.DEBUG
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s     %(name)s {%(module)s:%(lineno)d}  %(message)s"
    )
    handler.setFormatter(formatter)
    logger.setLevel(level)
    handler.setLevel(level)
    logger.addHandler(handler)

    return logger


def bootstrap(custom_headers: Dict[str, Any], metrics_counters: MetricsCounter):
    """Read service(s) from the bootstrapper"""
    if metrics_counters is None:
        raise BootstrapException("Metrics Counter not provided")

    href = os.environ.get("BOOTSTRAPPER_HREF", bootstrapper_default)

    try:
        # TODO - Should there be strict type validations for results ?
        results = rapp_session.get_json(
            href,
            rapp_session.retryable_http(custom_headers=custom_headers),
            BootstrapException,
            metrics_counters,
        )
    except (BootstrapException, ValueError, KeyError) as exc:
        metrics_counters.increment_bootstrap_failure_counter()
        return None, exc

    services = {}
    s = results["services"]
    for key in s:
        val = s[key]
        href = val["href"]
        services[key] = href

    metrics_counters.increment_bootstrap_success_counter()
    return services, None


def registry_uri(registry_href: str, service_name: str) -> str:
    """
    Method to take the input href and convert it to the
    canonical uri for accessing the registry service.
    :param registry_href: href of the registry host
    :return: canonical uri to reach the registry
    """
    return registry_href + "/services/" + service_name


def platform_uri(registry_href: str) -> str:
    """
    Method to take the input href and convert it to the
    canonical uri for requesting the platform services.
    :param registry_href: href of the registry host
    :return: canonical uri to request platform services
    """
    return registry_href + "/services?service_type=platform"


def consumer_registration_uri(dms_href: str, job_id: str) -> str:
    """
    Method to take the input href and convert it to the
    canonical uri for registering the job request of a
    consumer rapp.
    :param dms_href: href of the DMS host
    :param job_id: id of the job to be registered
    :return: canonical uri to register consumer jobs
    """
    return dms_href + "/info-jobs/" + job_id


def register(
    registry_href: str,
    service: Dict,
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
):
    """
    Build the canonical URI for registering the service
    and send a PUT request to the R1 registry using the
    given registry href and service.
    :param registry_href: href of the registry host
    :param service: service that needs to be registered
    :param custom_headers: custom headers to be passed for registration
    :param metrics_counters: metrics counter object
    :return: received response body
    """
    uri = registry_uri(registry_href, service["name"])
    try:
        results = rapp_session.put_json(
            uri,
            rapp_session.retryable_http(custom_headers=custom_headers),
            service,
            RegistryException,
            metrics_counters,
        )
    except (RegistryException, ValueError, KeyError) as exc:
        return None, exc

    return results, None


def timestamp_is_milliseconds(timestamp: int) -> bool:
    """
    Return true if this looks like milliseconds since the
    epoch.
    """
    return timestamp > ten_pow_twelve


def build_pm_history_job_definition(
    start_ms: int,
    end_ms: int,
    counter_names: List[str],
    cell_ids: List[str] = None,
    technology="nr",
) -> Dict:
    """
    build the job definition for PM history data for a window of time described
    by start_ms and end_ms

    Both start_ms and end_ms must be within 10 years of now.

    :param start_ms: Start time of the window, in milliseconds since the Unix
                     epoch.
    :param end_ms: End time of the window, in milliseconds since the Unix
                   epoch.
    :param counter_names: List of pmhistory counters to query
    :param cell_ids: Optional list of cell global ids to limit query to. If no
                     cell_ids are provided, counters are returned for all known
                     cells, limited by the counters in the counters parameter.
    :param technology: Optional technology, "nr" or "lte". If not stated, the
                       default is "nr".
    :return: PM history job definition
    """

    if not timestamp_is_milliseconds(start_ms):
        raise RappException(
            "start_ms <%d> must be milliseconds since the epoch" % start_ms
        )

    if not timestamp_is_milliseconds(end_ms):
        raise RappException("end_ms <%d> must be milliseconds since the epoch" % end_ms)

    if start_ms >= end_ms:
        raise RappException(
            "start_ms <%d> must be before end_ms <%d>" % (start_ms, end_ms)
        )

    # Define the PM history job data
    job_data = {
        "counter_names": counter_names,
        "start": int(start_ms),
        "end": int(end_ms),
        "technology": technology,
    }

    # Add cell IDs if specified, make sure they are strings.
    if cell_ids:
        job_data["cell_ids"] = list(map(str, cell_ids))

    return job_data


def build_pm_live_job_definition(
    counters: List[str],
    cell_ids: List[str],
    technology="nr",
) -> Dict:
    """
    build the job definition for PM live data
    :param counters: list of pm counters to query
    :param cell_ids: list of cell ids to query
    :return: PM live job definition
    """
    # Define the PM live job data
    job_data = {
        "counter_names": counters,
        "cell_ids": cell_ids,
        "technology": technology,
    }
    return job_data


def get_platform_services(
    registry_href: str,
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
) -> (Dict[str, str], Any):
    """
    Method to get the platform services from R1 registry by
     building the appropriate canonical uri from the given href.
    :param registry_href: href of the registry host
    :param custom_headers: custom headers for platform services
    :param metrics_counters: object to set the metrics counter
    :return: platform services as a dict
    """
    platform_services: Dict[str, str] = {}
    try:
        # TODO - Should there be strict type validation for results ?
        metrics_counters.increment_registry_listing_counter()
        results = rapp_session.get_json(
            platform_uri(registry_href),
            rapp_session.retryable_http(custom_headers=custom_headers),
            RegistryException,
            metrics_counters,
        )
    except (RegistryException, ValueError, KeyError) as exc:
        return None, exc

    # Parse the results and return
    if not results:
        return platform_services, None

    for service in results:
        name = service["name"]
        href = service["service_href"]
        platform_services[name] = href

    return platform_services, None


def announce_consumer_job(
    dms_href: str,
    job_id: str,
    job_info: Dict[str, Any],
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
):
    """
    Build the canonical URI for registering the consumer
    job and send a PUT request to the R1 DMS using the
    given registry href and service.
    :param dms_href: href of the DMS host
    :param job_id: ID associated with the job
    :param job_info: Info of the Job
    :param custom_headers: Custom headers for the job. Ex:user-agent
    :param metrics_counters: object to set the metrics counter
    :return: received response body
    """
    uri = consumer_registration_uri(dms_href, job_id)
    try:
        results = rapp_session.put_json(
            uri,
            rapp_session.retryable_http(custom_headers=custom_headers),
            job_info,
            DmsException,
            metrics_counters,
        )
    except (DmsException, ValueError, KeyError) as exc:
        return None, exc

    return results, None


def delete_consumer_job(dms_href: str, job_id: str, custom_headers: Dict[str, Any]):
    """
    Build the canonical URI for deleting the consumer
    job and send a DELETE request to the R1 DMS.
    :param dms_href: href of the DMS host
    :param job_id: ID associated with the job
    :param custom_headers: Custom headers for the job. Ex:user-agent
    """
    uri = consumer_registration_uri(dms_href, job_id)
    try:
        rapp_session.delete_request(
            uri,
            rapp_session.retryable_http(custom_headers=custom_headers),
            DmsException,
        )
    except (DmsException, ValueError, KeyError) as exc:
        return exc

    return None


def get_cmi_uri(
    dms_href: str,
    info_type: str,
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
):
    """
    Method to get the URI for interacting with CMI
    to read / write CM data.
    :param dms_href: href of the DMS
    :param info_type: info type to be included in the URI
    :param custom_headers: Custom headers for the job. Ex:user-agent
    :param metrics_counters: prometheus counters object
    :return: URI for CMI interaction.
    """
    dms_uri: str = dms_href + "/info-types/" + info_type
    try:
        results = rapp_session.get_json(
            dms_uri,
            rapp_session.retryable_http(custom_headers=custom_headers),
            CmiException,
            metrics_counters,
        )
    except (CmiException, ValueError, KeyError) as exc:
        return None, exc

    return list(results["job_data_schema"].keys())[0], None
