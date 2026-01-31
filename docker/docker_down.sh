echo "Tearing down Airflow containers..."

docker compose --file .airflow.dockerfile down --env-file .env down -v
