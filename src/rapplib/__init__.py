"""rapplib provides a collection of functions, classes and methods to enable
development of Python-based rApps for use in the VMware Centralized RIC.

The :py:class:`rapplib.consumer.Consumer` class provides a base class for
creating a consumer rApp, automating many onboarding and data-gathering steps.

The :py:class:`rapplib.heartbeat.HeartbeatManager` class automates handling
required heartbeat messages between an rApp and the Service Management and
Exposure service (hosted by the Data Management Service).

The :py:mod:`rapplib.rapp` module provides base functions for interacting with
R1 services. Many of these functions are used by the Consumer class. If
creating an rApp with the Consumer class is not providing sufficient flexibility
inspect this module.

The :py:mod:`rapplib.server` module provides the HTTP server that the rApp must
run to receive notifications from the R1 services. It is also responsible for
retrieving results from data producers in the environment and preparing results
to be passed through to the data-processing parts of an rApp.

Each module and class has additional documentation.
"""
