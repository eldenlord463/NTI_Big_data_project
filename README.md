# E-Commerce PySpark Pipeline — Split into 3 Notebooks + Airflow

Your original single notebook (`ecommerce_pyspark_project2.ipynb`, 97 cells)
split into 3 stage notebooks, orchestrated by an Airflow DAG. **No original
logic was changed** — every original cell was copied verbatim (diffed
cell-by-cell to confirm), except the single dependency-install cell, which
was intentionally revised per-notebook (see below, at your request). All
other additions are small "glue" cells, clearly marked
`# --- Added for orchestration ---`, needed so each notebook can run
independently instead of sharing one kernel's memory.

## Folder structure

```
ecommerce_pyspark_pipeline/
├── preprocessing_EDA/
│   └── Pre_and_eda.ipynb      Stage 1 — original cells 1–46 (setup, data
│                              understanding, data quality, EDA's 3 plots,
│                              preprocessing, feature engineering incl.
│                              session-level table). Ends by saving
│                              df_clean.parquet + session_dataset.parquet.
├── ml/
│   └── ML_models.ipynb        Stage 2 — original cells 47–69 (regression,
│                              classification, the 5 ML plots, results
│                              summary, conclusions). Loads
│                              session_dataset.parquet instead of re-deriving it.
├── sql/
│   └── SQL_analysis.ipynb     Stage 3 — original cells 70–96 (SQL 1–10 +
│                              their visualizations). Loads df_clean.parquet
│                              and session_dataset.parquet, registers them
│                              as the same `events` / `sessions` temp views
│                              the original queries use.
└── dags/
    └── ecommerce_pipeline_dag.py   Airflow DAG that runs the 3 notebooks
                                    in dependency order via papermill.
```

## Why this split

- **ML** only ever touches `session_dataset` (never re-reads the raw HDFS CSV).
- **SQL** only ever touches `df_clean` and `session_dataset`.
- Both depend on Stage 1, but not on each other → they can run **in
  parallel** once Stage 1 finishes.

## Dependency install (revised, per-notebook)

The original notebook had one install cell (cell 0) that installed both
`pyarrow` and `xgboost[spark]` for the whole thing. Since each split notebook
runs in its own kernel, that cell was replaced with a **scoped install cell
in each notebook**, installing only what that notebook actually calls:

| Notebook | Installs | Why |
|---|---|---|
| `Pre_and_eda.ipynb` | `pyarrow` | EDA plots call `.toPandas()`; no xgboost used |
| `ML_models.ipynb` | `pyarrow`, `xgboost[spark]` | `.toPandas()` for plots **and** `SparkXGBRegressor`/`SparkXGBClassifier` |
| `SQL_analysis.ipynb` | `pyarrow` | Each SQL query's viz calls `.toPandas()`; no xgboost used |

Both packages are on PyPI (verified available) — `pyarrow` and `xgboost`
(whose `[spark]` extra provides `xgboost.spark`).

## Data hand-off between notebooks

Stage 1 writes:
- `df_clean.parquet`
- `session_dataset.parquet`

to `DATA_DIR` (default `/tmp/ecommerce_pipeline/data`, overridable with the
`ECOMM_DATA_DIR` env var). Stages 2 and 3 read them back with
`spark.read.parquet(...)` in their setup cell instead of rebuilding them.

## Running manually (without Airflow)

Each notebook is fully standalone — just run it top to bottom:

1. `preprocessing_EDA/Pre_and_eda.ipynb`
2. `ml/ML_models.ipynb` and `sql/SQL_analysis.ipynb` (either order, or in
   parallel)

## Running with Airflow

```
ecommerce_pipeline_dag.py
        preprocess_and_eda
              /    \
      ml_models   sql_analysis   (parallel)
```

1. Copy `ecommerce_pyspark_pipeline/` into your Airflow `dags/` folder (or
   set `ECOMM_PROJECT_ROOT` to wherever you put it).
2. On the workers: `pip install papermill ipykernel pyspark`
   (the notebooks themselves install `pyarrow` / `xgboost[spark]` in their
   own scoped install cell — see above).
3. Make sure `DATA_DIR` is a path every worker/task can read/write (a shared
   volume, or an HDFS path if you point `DATA_DIR` at `hdfs://...`).
4. Trigger the `ecommerce_pyspark_pipeline` DAG in the Airflow UI, or:
   ```
   airflow dags trigger ecommerce_pyspark_pipeline
   ```

Each task executes its notebook with papermill and saves a fully-executed
copy under `executed_runs/` for later inspection — if a notebook errors, the
task (and DAG) fails.

## Notes

- The original notebook had `HDFS_PATH = "hdfs://namenode:9000/test/2019-Oct.csv"`
  hard-coded in Stage 1 — left as-is, since you asked for no code edits.
  Point it at your actual HDFS path if it differs.
- Stage 1's Spark session setup has a pre-existing paren/indentation quirk
  in the original notebook (the `spark.conf.set(...)` calls sit inside the
  same `(...)` as the `SparkSession.builder` chain) — also left untouched,
  since that's your original code, not something introduced by the split.
