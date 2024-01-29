.PHONY: build, up, down, pgcli, ingest

compose-file = local-setup.yml

build:
	docker-compose -f $(compose-file) build --no-cache

up:
	docker-compose -f $(compose-file) up -d

down:
	docker-compose -f $(compose-file) down

# clean:
# 	docker rmi 01-docker-terraform_pgcli | \
# 	docker volume rm 01-docker-terraform_ny_taxi_postgres_data

pgcli:
	docker exec -it 01-docker-terraform_python_1 pgcli -h db -d ny_taxi -p 5432 -u root

# ingest:
# 	docker exec -w /app/Zoomcamp24/01-docker-terraform 01-docker-terraform_python_1 python ingest_data.py \
# 	--user=root \
# 	--password=root \
# 	--host=01-docker-terraform_db_1 \
# 	--port=5432 \
# 	--db=ny_taxi \
# 	--table_name=yellow_taxi_data \
# 	--url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz


ingest:
	python ingest_data.py \
	--user=root \
	--password=root \
	--host=01-docker-terraform_db_1 \
	--port=5432 \
	--db=ny_taxi \
	--table_name=green_taxi_data \
	--url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz