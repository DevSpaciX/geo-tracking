.PHONY: up down logs test test-unit load migrate

up:            ## build and start the whole stack
	docker compose up --build -d

down:          ## stop and remove containers + volumes
	docker compose down -v

logs:
	docker compose logs -f app

test:          ## run the full test-suite (incl. PostGIS integration tests) in Docker
	docker compose --profile test run --build --rm test

test-unit:     ## run DB-free tests locally
	pytest -q -m "not db"

load:          ## run the 10k-device generator inside the compose network
	docker compose --profile load up --build generator

migrate:
	docker compose run --rm migrate
