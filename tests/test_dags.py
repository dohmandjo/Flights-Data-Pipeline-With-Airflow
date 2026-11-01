"""DAG-level checks: they import cleanly and every task retries with backoff.

Skipped unless apache-airflow is installed (CI installs it).
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")  # "airflow" alone would match the repo's airflow/ folder

from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_exist(dagbag):
    assert {"warehouse_load_every_2_min", "gsheet_incremental_sync"} <= set(dagbag.dag_ids)


def test_every_task_retries_with_exponential_backoff(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.retries >= 3, f"{dag.dag_id}.{task.task_id} has retries={task.retries}"
            assert task.retry_exponential_backoff, f"{dag.dag_id}.{task.task_id} has no backoff"
            assert task.execution_timeout is not None, f"{dag.dag_id}.{task.task_id} has no timeout"


def test_runs_never_overlap_and_do_not_backfill(dagbag):
    for dag in dagbag.dags.values():
        assert dag.max_active_runs == 1, dag.dag_id
        assert dag.catchup is False, dag.dag_id


def test_warehouse_load_runs_in_a_single_transaction(dagbag):
    """The snapshot isolation statement only works when all statements share one transaction."""
    task = dagbag.dags["warehouse_load_every_2_min"].get_task("upsert_from_staging")
    assert task.autocommit is False
    assert task.sql[0].startswith("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
