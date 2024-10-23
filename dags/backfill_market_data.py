import pathlib
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from pandas import Timestamp
from tradingo import sampling

from tradingo.cli import resolve_config


os.environ["IG_SERVICE_ACC_TYPE"] = Variable.get("IG_SERVICE_ACC_TYPE")
os.environ["IG_SERVICE_PASSWORD"] = Variable.get("IG_SERVICE_PASSWORD")
os.environ["IG_SERVICE_USERNAME"] = Variable.get("IG_SERVICE_USERNAME")
os.environ["IG_SERVICE_API_KEY"] = Variable.get("IG_SERVICE_API_KEY")
os.environ["IG_SERVICE_API_KEY"] = Variable.get("IG_SERVICE_API_KEY")


START_DATE = "2024-01-01"

HOME_DIR = pathlib.Path(os.environ["AIRFLOW_HOME"]) / "config" / "trading"
config = resolve_config(HOME_DIR / "ig-trading.json")


for i, (universe, config) in enumerate(config["universe"].items()):

    # TODO: start,end date
    #
    #
    with DAG(
        dag_id=f"market_data.{universe}",
        start_date=Timestamp(START_DATE),
        schedule="0 0 * * 1-5",
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
    ) as dag:

        provider = config["provider"]

        _ = (
            #     PythonOperator(
            #     task_id="instruments",
            #     python_callable=sampling.download_instruments,
            #     op_args=[],
            #     op_kwargs={
            #         "html": config.get("html"),
            #         "file": config.get("file"),
            #         "tickers": config.get("tickers"),
            #         "epics": config.get("epics"),
            #         "index_col": config["index_col"],
            #         "universe": universe,
            #     },
            # ) >>
            PythonOperator(
                task_id="sample",
                python_callable=sampling.sample_ig_instruments,
                op_args=[],
                op_kwargs={
                    "start_date": "{{ data_interval_start }}",
                    "end_date": "{{ data_interval_end }}",
                    "provider": provider,
                    "interval": config.get("interval", "1d"),
                    "universe": universe,
                    "periods": config.get("periods"),
                },
            )
        )

    globals()[f"dag_{i}"] = dag
