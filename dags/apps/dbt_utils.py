import os

from airflow.models import Variable
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig
)
from cosmos.constants import (
    TestBehavior,
    ExecutionMode,
    LoadMode
)
from cosmos.dbt.graph import DbtGraph
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping 


def _get_profile_config(target_name: str) -> ProfileConfig:
    return ProfileConfig(
        profile_name="star",
        target_name=target_name,
        profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
            project_id=Variable.get("gcp_project_id"),
            keyfile_path=Variable.get("gcp_key_path"),
            profile_args={
                "dataset": "dbt"  # _intermediate and _marts will append to base dataset name
            }
        )
    )


def _get_project_config(dbt_project_path: str) -> ProjectConfig:
    return ProjectConfig(
        dbt_project_path=dbt_project_path,
        manifest_path=f"{dbt_project_path}/target/manifest.json",
        env_vars={
            "AIRFLOW_VAR_GCP_PROJECT_ID": Variable.get("gcp_project_id"),
            "AIRFLOW_VAR_GCP_KEY_PATH": Variable.get("gcp_key_path")
        },
        install_dbt_deps=True
    )


def _get_render_config(select: list[str] = None) -> RenderConfig:
    return RenderConfig(
        select=select,
        emit_datasets=False,
        test_behavior=TestBehavior.AFTER_EACH,
        load_method=LoadMode.DBT_MANIFEST,
    )


def _get_execution_config(dbt_executable_path: str) -> ExecutionConfig:
    return ExecutionConfig(
        dbt_executable_path
    )


def create_dag_dbt_run_schedule(select: list[str], trigger_rule: str = "all_success"):
    profile_config = _get_profile_config(target_name="prod")
    project_config = _get_project_config(dbt_project_path="/opt/airflow/dags/dbt/star")
    render_config = _get_render_config(select=select)
    execution_config = _get_execution_config(dbt_executable_path="/usr/local/airflow/dbt-env/bin/dbt")
    dbt_run_models_task_group = DbtTaskGroup(
        group_id="dbt_run_models_task_group",
        project_config=project_config,
        render_config=render_config,
        execution_config=execution_config,
        profile_config=profile_config,
        opreator_args={"fail_fast": True, "cancel_query_on_kill": True, "trigger_rule": trigger_rule, "dbt_cmd_global_flags": ["--debug"]}
    )
    return dbt_run_models_task_group


def get_dbt_graph():
    profile_config = _get_profile_config(target_name="prod")
    project_config = _get_project_config(dbt_project_path="/opt/airflow/dags/dbt/star")
    render_config = _get_render_config()
    execution_config = _get_execution_config(dbt_executable_path="/usr/local/airflow/dbt-env/bin/dbt")
    dbt_graph = DbtGraph(
        project=project_config,
        render_config=render_config,
        profile_config=profile_config,
        execution_config=execution_config
    )
    dbt_graph.load()
    return dbt_graph
