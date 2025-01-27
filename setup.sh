docker compose -f ./setup/airflow/docker-compose.yml down -v
docker compose -f ./setup/airflow/docker-compose.yml up --build --detach --force-recreate

docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml
docker exec -it airflow-webserver airflow variables import -a overwrite /init/variables_and_connections/airflow_variables_init.json