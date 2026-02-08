import os
import pendulum


def get_default_args() -> dict:
    """
    Returns a set of default parameters for DAGs.

    @return: Dictionary of default parameters for DAGs.
    """
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "retry_delay": pendulum.duration(minutes=1),
        "retries": 1
    }


def get_dag_name(dag_file_path: str) -> str:
    """
    Generates a DAG name based on the file path of DAG.

    @return: String representing DAG ID.
    """
    dag_dir_prefix = os.path.join("dags", "constructors")
    dag_path = os.path.relpath(path=dag_file_path, start=dag_dir_prefix)
    return dag_path.split(dag_dir_prefix)[-1].split(".")[0].replace("/", "_")
