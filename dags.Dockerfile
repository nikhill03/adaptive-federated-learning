FROM apache/airflow:2.9.3
ARG COMPONENT_IMAGE_VERSION

ENV COMPONENT_IMAGE_VERSION=$COMPONENT_IMAGE_VERSION
USER airflow
COPY --chown=airflow:root dags/*.py /opt/airflow/dags/