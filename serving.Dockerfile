FROM python:3.12-slim-bookworm
RUN apt update && apt install -y git vim curl

RUN mkdir -p /opt/oran
WORKDIR /opt/oran
RUN pip3 install Flask numpy joblib pandas tensorflow[and-cuda] scikit-learn
COPY model_serving/ /opt/oran/
CMD python3 /opt/oran/main.py