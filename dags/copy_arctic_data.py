from arcticdb import Arctic
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd


library = "prices"
symbol_regex = "^ig-trading"

# source_uri = "lmdb:///home/rory/dev/airflow/test/arctic.db"
target_uri = (
    "s3://s3.us-east-1.amazonaws.com:tradingo-store?aws_auth=true&path_prefix=prod"
)
source_uri = "lmdb:///opt/data/tradingo.db"


def sync_symbols(
    library,
    symbol_regex,
    source_uri,
    target_uri,
    as_of=None,
):

    source_a = Arctic(source_uri)
    target_a = Arctic(target_uri)

    source_lib = source_a.get_library(library)
    target_lib = target_a.get_library(library, create_if_missing=True)

    for sym in source_lib.list_symbols(regex=symbol_regex):

        data = source_lib.read(sym, as_of=pd.Timestamp(as_of) if as_of else None)
        target_lib.update(sym, data.data, upsert=True)


dag = DAG("sync_arctic_to_s3", start_date=pd.Timestamp("2024-10-16 00:00:00+00:00"))

with dag:

    for library in (
        "prices",
        "portfolio",
        "backtest",
        # "trades",
        "signals",
        "instruments",
    ):

        PythonOperator(
            task_id=library,
            python_callable=sync_symbols,
            op_kwargs={
                "library": library,
                "symbol_regex": "^ig-trading",
                "source_uri": source_uri,
                "target_uri": target_uri,
                # "as_of": "{{ data_interval_end }}",
            },
        )
