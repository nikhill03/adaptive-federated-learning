FROM python:3.12-slim-bookworm
ENV ROLE=client
RUN mkdir -p /opt/oran/src/rapplib/
WORKDIR /opt/oran
RUN apt update; apt install -y git vim curl;
COPY requirements.txt /opt/oran/
RUN python3 -m pip install -r requirements.txt

COPY ./src/*.py /opt/oran/src/
COPY ./src/rapplib/*.py /opt/oran/src/rapplib/
COPY ./src/training_set.csv /opt/oran/src/

WORKDIR /opt/oran/src
CMD python3 /opt/oran/src/main.py
