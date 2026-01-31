echo "Tearing down Airflow containers..."

docker compose --file docker-compose.yml --env-file .env down -v
