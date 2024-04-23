test:
		dotenv -f .env.test run -- pytest -vv -m "not staging"

staging:
		dotenv -f .env.staging run -- pytest -vv -m staging

clean:
		dotenv -f .env.staging run -- python clean_sf.py

start:
		dotenv -f .env.production run -- python -m omnivore

dashboard-dev:
		@dotenv -f .env.dashboard.dev run -- flask run --debug

dashboard-dev-db-init:
		@dotenv -f .env.dashboard.dev run -- flask db init

dashboard-dev-migrate:
		@dotenv -f .env.dashboard.dev run -- flask db migrate -m "${m}"

dashboard-dev-upgrade:
		@dotenv -f .env.dashboard.dev run -- flask db upgrade

dashboard-dummy:
		@dotenv -f .env.dashboard.dev run -- flask dummies 