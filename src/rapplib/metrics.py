"""
Provides common code for metrics information
"""

from prometheus_client import Counter

DEFAULT_METRIC_LABELS = ["metric_type", "source_type", "source_id", "source_vendor"]


class MetricsCounter:
    """
    Class to declare the common metrics counters
     and define methods to manipulate them.
    """

    def __init__(self, service_name: str = ""):
        self.number_of_successful_bootstrap_requests = Counter(
            "bootstrap_requests_success_total",
            "Accumulating count of how many bootstrapper requests have succeeded.",
            DEFAULT_METRIC_LABELS,
        )
        self.number_of_failed_bootstrap_requests = Counter(
            "bootstrap_requests_failed_total",
            "Accumulating count of how many bootstrapper requests have failed.",
            DEFAULT_METRIC_LABELS,
        )
        self.number_of_registry_listings = Counter(
            "registry_listings_total",
            "Accumulating count of how many registry listing requests have been made.",
            DEFAULT_METRIC_LABELS,
        )
        self.number_of_http_error_responses_by_status = Counter(
            "http_errors_by_status_total",
            "Accumulating counts of HTTP error responses, by HTTP status code.",
            DEFAULT_METRIC_LABELS + ["status_code"],
        )
        self.job_result_counter = Counter(
            "job_result_counter_total",
            "Accumulating count of how many pmhistory job result data have "
            "been fetched.",
            labelnames=DEFAULT_METRIC_LABELS + ["data_type"],
        )
        self.default_labels = {
            "source_id": service_name,
            "source_type": "RAPP",
            "metric_type": "Counter",
            "source_vendor": "VMware",
        }

    def increment_bootstrap_success_counter(self):
        self.number_of_successful_bootstrap_requests.labels(**self.default_labels).inc()

    def increment_bootstrap_failure_counter(self):
        self.number_of_failed_bootstrap_requests.labels(**self.default_labels).inc()

    def increment_registry_listing_counter(self):
        self.number_of_registry_listings.labels(**self.default_labels).inc()

    def increment_job_result_counter(self, data_type: str):
        self.job_result_counter.labels(**self.default_labels, data_type=data_type).inc()

    def increment_http_error_response_counter(self, status_code: int):
        self.number_of_http_error_responses_by_status.labels(
            **self.default_labels, status_code=str(status_code)
        ).inc()
