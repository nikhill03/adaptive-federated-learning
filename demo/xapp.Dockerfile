FROM python:3.12-slim-bookworm
RUN apt update && apt install -y git vim curl iputils-ping

RUN mkdir -p /opt/demo
WORKDIR /opt/demo
RUN pip3 install Flask pandas requests
COPY xapp.py  utils.py /opt/demo/
CMD python3 /opt/demo/xapp.py