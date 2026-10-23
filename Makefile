-include .env
export

.PHONY: init up down build logs ps trigger psql dashboard clean

init:
	@test -f .env || (echo "Создайте файл .env"; exit 1)

up: init
	docker compose up -d --build

down:
	docker compose down

build: init
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

trigger: init
	docker compose exec app airflow dags trigger $(ROZA_DAG_ID)

dashboard: init
	docker compose exec -T app python /app/bootstrap_dashboard.py

psql: init
	docker compose exec app /usr/lib/postgresql/15/bin/psql "postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:5432/$(POSTGRES_DB)"

clean:
	docker compose down -v --remove-orphans
