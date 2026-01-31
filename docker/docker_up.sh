echo "Creating Airflow containers..."

docker compose --file .airflow.dockerfile --env-file .env up -d
