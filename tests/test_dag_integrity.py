import pendulum
from airflow import DAG
from apps.file_loader import BigQueryFileLoader


def test_file_loader_dag_structure():
    """
    Validates BigQueryFileLoader correctly creates the expected tasks in a DAG.
    """
    with DAG(dag_id="test_loader_dag", start_date=pendulum.now()) as dag:
        file_loader = BigQueryFileLoader()
        file_loader.set_parameters(
            table_dataset_id="test_dataset",
            table_id="test_table",
            operation="merge",
            primary_keys=["id"],
            rows_to_skip=1,
        )
        file_loader.build_dag()
    # Verify tasks exist in the DAG
    task_ids = dag.task_ids
    expected_tasks = [
        "fetch_ingress_ods_schema",
        "get_most_recent_ingress_file",
        "archive_most_recent_ingress_file",
        "load_archive_file_to_ingress_table",
        "write_ingress_to_ods",
    ]
    for task_id in expected_tasks:
        assert task_id in task_ids, f"Task {task_id} missing from DAG"

    # Verify dependencies (linear chain in build_dag)
    tasks = {t.task_id: t for t in dag.tasks}
    assert (
        tasks["get_most_recent_ingress_file"]
        in tasks["fetch_ingress_ods_schema"].downstream_list
    )
    assert (
        tasks["archive_most_recent_ingress_file"]
        in tasks["get_most_recent_ingress_file"].downstream_list
    )
    assert (
        tasks["archive_most_recent_ingress_file"]
        in tasks["get_most_recent_ingress_file"].downstream_list
    )
