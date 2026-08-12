"""
Airflow DAG: e-commerce PySpark pipeline (split notebooks)

Orchestrates the 3 notebooks produced by splitting the original
monolithic notebook:

    preprocessing_EDA/Pre_and_eda.ipynb   -> writes df_clean.parquet, session_dataset.parquet
    ml/ML_models.ipynb                    -> reads session_dataset.parquet
    sql/SQL_analysis.ipynb                -> reads df_clean.parquet, session_dataset.parquet

Dependency graph:

    preprocess_and_eda
          |
      -------
      |     |
    ml_models  sql_analysis     (run in parallel, both depend only on stage 1)

Each task executes its notebook in-place with papermill (so the run
gets a timestamped, fully-executed output notebook you can inspect
afterwards) and fails the task if the notebook raises an exception.

Requirements on the Airflow workers:
    pip install papermill ipykernel pyspark pyarrow xgboost[spark] matplotlib numpy

Configure the shared data directory (must be reachable by every task,
e.g. a mounted volume or HDFS-backed path) either via the
ECOMM_DATA_DIR env var on the workers, or by editing DATA_DIR below.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
PROJECT_ROOT = os.environ.get(
    "ECOMM_PROJECT_ROOT",
    "/opt/airflow/dags/ecommerce_pyspark_pipeline",
)
DATA_DIR = os.environ.get("ECOMM_DATA_DIR", "/tmp/ecommerce_pipeline/data")
OUTPUT_DIR = os.environ.get(
    "ECOMM_NOTEBOOK_OUTPUT_DIR",
    "/opt/airflow/dags/ecommerce_pyspark_pipeline/executed_runs",
)

NOTEBOOKS = {
    "preprocess_and_eda": f"{PROJECT_ROOT}/preprocessing_EDA/Pre_and_eda.ipynb",
    "ml_models": f"{PROJECT_ROOT}/ml/ML_models.ipynb",
    "sql_analysis": f"{PROJECT_ROOT}/sql/SQL_analysis.ipynb",
}

default_args = {
    "owner": "ahmed",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_notebook(task_key: str, **context):
    """Execute one notebook with papermill and raise on failure."""
    import papermill as pm

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    run_ts = context["ts_nodash"]
    input_path = NOTEBOOKS[task_key]
    output_path = f"{OUTPUT_DIR}/{task_key}_{run_ts}.ipynb"

    # Each notebook's setup cell reads DATA_DIR via
    # os.environ.get("ECOMM_DATA_DIR", ...), so we just need this
    # process's env set before spawning the papermill kernel -- no
    # need for a papermill "parameters" cell tag since we didn't
    # touch the original notebook cells.
    os.environ["ECOMM_DATA_DIR"] = DATA_DIR

    pm.execute_notebook(
        input_path=input_path,
        output_path=output_path,
        kernel_name="python3",
    )
    return output_path


with DAG(
    dag_id="ecommerce_pyspark_pipeline",
    description="Preprocessing/EDA -> (ML + SQL in parallel), split from the original single notebook",
    default_args=default_args,
    schedule=None,  # trigger manually, or set e.g. "@daily"
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pyspark", "ecommerce", "papermill"],
) as dag:

    preprocess_and_eda = PythonOperator(
        task_id="preprocess_and_eda",
        python_callable=_run_notebook,
        op_kwargs={"task_key": "preprocess_and_eda"},
    )

    ml_models = PythonOperator(
        task_id="ml_models",
        python_callable=_run_notebook,
        op_kwargs={"task_key": "ml_models"},
    )

    sql_analysis = PythonOperator(
        task_id="sql_analysis",
        python_callable=_run_notebook,
        op_kwargs={"task_key": "sql_analysis"},
    )

    # Stage 1 must finish (and write the parquet files) before either
    # Stage 2 or Stage 3 can read them. Stage 2 and Stage 3 don't
    # depend on each other, so they run in parallel.
    preprocess_and_eda >> [ml_models, sql_analysis]
