echo "Creating Airflow containers..."

docker compose --file ./dev_env/docker-compose.yml --env-file .env up -d
