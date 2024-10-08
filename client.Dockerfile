FROM python:3.12-slim-bookworm as build
RUN apt update; apt install -y git vim curl;
WORKDIR /opt
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN git clone https://github.com/nimbus-gateway/cords-semantics-lib.git
RUN cd cords-semantics-lib && /root/.local/bin/poetry build
#--------
FROM python:3.12-slim-bookworm
ENV MLFLOW_TRACKING_URI=http://10.180.113.115:32256
ENV MLFLOW_TRACKING_USERNAME=user
ENV MLFLOW_TRACKING_PASSWORD=sr9TvkIjaj
ENV INFLUXDB_TOKEN=txGhRR5wrAccsSSMLdPUQFZjbfEUyXEjnzywkUiFjSST84hc9CxXsyigTBrsHazVDAJi_47R-vQWUyJiBwawQQ==

RUN apt update && apt install -y git vim curl

COPY --from=build /opt/cords-semantics-lib/dist/cords_semantics-0.2.1.tar.gz /opt

RUN mkdir -p /opt/oran
WORKDIR /opt/oran
RUN pip3 install numpy pandas flwr mlflow tensorflow[and-cuda]
RUN pip3 install /opt/cords_semantics-0.2.1.tar.gz
COPY fl_client/*.py fl_client/*.csv /opt/oran/
CMD python3 /opt/oran/main.py