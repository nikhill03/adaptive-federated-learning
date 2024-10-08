FROM python:3.12-slim-bookworm
RUN apt update && apt install -y git vim curl iputils-ping

RUN mkdir -p /opt/demo
WORKDIR /opt/demo
RUN pip3 install Flask pandas requests influxdb-client
COPY ue.py  utils.py *.csv /opt/demo/
CMD python3 /opt/demo/ue.py