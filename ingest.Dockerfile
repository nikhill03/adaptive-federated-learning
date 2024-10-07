FROM python:3.12-slim-bookworm
RUN apt update &&  apt install -y git vim curl
RUN mkdir -p /opt/oran
WORKDIR /opt/oran

COPY ingest/ /opt/oran/
