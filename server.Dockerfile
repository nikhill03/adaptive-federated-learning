FROM python:3.12-slim-bookworm
RUN apt update && apt install -y git vim curl

RUN mkdir -p /opt/oran
WORKDIR /opt/oran
RUN pip3 install matplotlib flwr
COPY fl_server/*.py /opt/oran/
CMD python3 /opt/oran/main.py