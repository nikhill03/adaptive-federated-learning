FROM python:3.9-slim-bookworm
ENV FL_ROLE=client

RUN mkdir -p /opt/oran
WORKDIR /opt/oran
COPY requirements.txt /opt/oran
RUN apt update; apt install -y git vim curl;
RUN python3 -m pip install -r requirements.txt

COPY . /opt/oran

CMD python3 /opt/oran/$FL_ROLE.py
