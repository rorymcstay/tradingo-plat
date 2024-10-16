FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

ARG TRADINGO_VERSION=0.0.1

RUN pip install apache-airflow==${AIRFLOW_VERSION} tradingo==${TRADINGO_VERSION}

