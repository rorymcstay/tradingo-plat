from arcticdb import Arctic
from airflow import DAG
from airflow.operators.python import PythonOperator


library = "prices"
symbol_regex = "^ig-trading"

# source_uri = "lmdb:///home/rory/dev/airflow/test/arctic.db"
target_uri = (
    "s3://s3.us-east-1.amazonaws.com:tradingo-store?aws_auth=true&path_prefix=prod"
)
source_uri = "lmdb:///home/rory/dev/tradingo-plat/data/prod/tradingo.db"


def sync_symbols(
    library,
    symbol_regex,
    source_uri,
    target_uri,
):

    source_a = Arctic(source_uri)
    target_a = Arctic(target_uri)

    source_lib = source_a.get_library(library)
    target_lib = target_a.get_library(library, create_if_missing=True)

    for sym in source_lib.list_symbols(regex=symbol_regex):

        data = source_lib.read(sym)
        target_lib.write(sym, data.data)


dag = DAG("sync_arctic_to_s3")

with dag:

    for library in (
        "prices",
        "portfolio",
        "backtest",
        "trades",
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
            },
        )
