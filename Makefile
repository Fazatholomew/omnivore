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

dashboard-dummy:
		@dotenv -f .env.dashboard.dev run -- flask dummies 