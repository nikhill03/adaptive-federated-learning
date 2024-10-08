FROM python:3.12-slim-bookworm
RUN apt update && apt install -y git vim curl

RUN mkdir -p /opt/demo
WORKDIR /opt/demo
RUN pip3 install Flask pandas requests
COPY bs.py utils.py /opt/demo/
CMD python3 /opt/demo/bs.py