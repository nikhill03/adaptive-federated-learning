"""
Provides common code for data producers and consumers
that operate as rApps in the Non-Real-Time RIC environment.
It uses asynchronous HTTP requests.
"""

from rapplib.metrics import MetricsCounter
from rapplib import rapp_async_session, rapp
from typing import Dict, Any


async def register(
    registry_href: str,
    service: Dict,
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
):
    """
    Build the canonical URI for registering the service
    and send a PUT request to the R1 registry using the
    given registry href and service. It uses async HTTP
    requests.
    :param registry_href: href of the registry host
    :param service: service that needs to be registered
    :param custom_headers: custom headers to be passed for registration
    :param metrics_counters: metrics counter object
    :return: received response body
    """
    uri = rapp.registry_uri(registry_href, service["name"])
    try:
        json_results, _ = await rapp_async_session.put_json(
            rapp_async_session.get_session(custom_headers=custom_headers),
            uri,
            service,
            rapp.RegistryException,
            metrics_counters,
        )
    except (rapp.RegistryException, ValueError, KeyError) as exc:
        return None, exc

    return json_results, None


async def announce_consumer_job(
    dms_href: str,
    job_id: str,
    job_info: Dict[str, Any],
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
):
    """
    Build the canonical URI for registering the consumer
    job and send a PUT request to the R1 DMS using the
    given registry href and service. It uses async HTTP
    requests.
    :param dms_href: href of the DMS host
    :param job_id: ID associated with the job
    :param job_info: Info of the Job
    :param custom_headers: Custom headers for the job. Ex:user-agent
    :param metrics_counters: object to set the metrics counter
    :return: received response body
    """
    uri = rapp.consumer_registration_uri(dms_href, job_id)
    try:
        json_results, _ = await rapp_async_session.put_json(
            rapp_async_session.get_session(custom_headers=custom_headers),
            uri,
            job_info,
            rapp.DmsException,
            metrics_counters,
        )
    except (rapp.DmsException, ValueError, KeyError) as exc:
        return None, exc

    return json_results, None


async def delete_consumer_job(
    dms_href: str, job_id: str, custom_headers: Dict[str, Any]
):
    """
    Build the canonical URI for deleting the consumer
    job and send a DELETE request to the R1 DMS. It uses async HTTP
    request.
    :param dms_href: href of the DMS host
    :param job_id: ID associated with the job
    :param custom_headers: Custom headers for the job. Ex:user-agent
    """
    uri = rapp.consumer_registration_uri(dms_href, job_id)
    try:
        await rapp_async_session.delete_request(
            rapp_async_session.get_session(custom_headers=custom_headers),
            uri,
            rapp.DmsException,
        )
    except (rapp.DmsException, ValueError, KeyError) as exc:
        return exc

    return None


async def get_cmi_uri(
    dms_href: str,
    info_type: str,
    custom_headers: Dict[str, Any],
    metrics_counters: MetricsCounter,
):
    """
    Method to get the URI for interacting with CMI
    to read / write CM data. It uses async HTTP requests.
    :param dms_href: href of the DMS
    :param info_type: info type to be included in the URI
    :param custom_headers: Custom headers for the job. Ex:user-agent
    :param metrics_counters: prometheus counters object
    :return: URI for CMI interaction.
    """
    dms_uri: str = dms_href + "/info-types/" + info_type
    try:
        json_results, _ = await rapp_async_session.get_json(
            rapp_async_session.get_session(custom_headers=custom_headers),
            dms_uri,
            rapp.CmiException,
            metrics_counters,
        )
    except (rapp.CmiException, ValueError, KeyError) as exc:
        return None, exc

    return list(json_results["job_data_schema"].keys())[0], None
