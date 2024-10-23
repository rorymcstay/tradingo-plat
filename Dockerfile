FROM apache/airflow:2.10.2-python3.11

# TODO: manylinux wheel for tradingo package, so that we dont have to install gcc
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

ARG TRADINGO_VERSION=0.0.15

RUN pip install apache-airflow==${AIRFLOW_VERSION} tradingo[research]==${TRADINGO_VERSION}

