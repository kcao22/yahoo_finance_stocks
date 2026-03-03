import yaml
from collections import defaultdict

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from apps import af_utils, gcp_utils


@dag(
    dag_id=af_utils.get_dag_name(__file__),
    default_args=af_utils.get_default_args(),
    schedule_interval=None,
    catchup=False,
    params={"delete_tables": Param(default=False, type="boolean")},
)
def dag():

    @task
    def drop_and_create_tables():
        context = get_current_context()
        delete_tables = context["params"]["delete_tables"]
        with open("/opt/airflow/config/bq_schemas.yaml", "r") as infile:
            config = yaml.safe_load(infile)
            for source in config["sources"]:
                dataset_groups = defaultdict(list)
                source_name = source["name"]
                for table_config in source["tables"]:
                    dataset_id = f"{table_config['dataset_type']}_{source_name}"
                    dataset_groups[dataset_id].append(table_config)
                for dataset_id, tables_to_check in dataset_groups.items():
                    existing_table_names = gcp_utils.list_bigquery_tables(
                        dataset_id=dataset_id
                    )
                    for table_config in tables_to_check:
                        table_id = table_config["table_name"]
                        table_exists = table_id in existing_table_names
                        if table_exists and delete_tables:
                            gcp_utils.delete_bigquery_table(
                                dataset_id=dataset_id,
                                table_id=table_config["table_name"],
                            )
                            table_exists = False
                        if not table_exists or delete_tables:
                            parsed_schema = gcp_utils.parse_bq_schema(
                                schema_config=table_config["schema"]
                            )
                            gcp_utils.create_bigquery_table(
                                dataset_id=dataset_id,
                                table_id=table_config["table_name"],
                                schema=parsed_schema,
                                partition_field=table_config.get("partition_field"),
                                clustering_fields=table_config.get("clustering_fields"),
                            )
                        else:
                            print(
                                f"Table {dataset_id}.{table_id} already exists. Skipping creation..."
                            )

    drop_and_create_tables()


dag()
