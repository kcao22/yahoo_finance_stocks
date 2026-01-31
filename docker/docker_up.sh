echo "Creating Airflow containers..."

docker compose --file docker-compose.yml --env-file .env up -d
