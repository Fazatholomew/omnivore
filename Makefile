test.cmd="pytest -vv -m \"not staging\""
test.env_file=".env.test"
staging.cmd="pytest -vv -m staging"
staging.env_file=".env.staging"
clean.cmd="python clean_sf.py"
clean.env_file=".env.staging"
bulk.cmd="python bulk.py"
bulk.env_file=".env.staging"
start.cmd="python -m omnivore"
start.env_file=".env.production"

test:
		dotenv -f .env.test run -- pytest -vv -m "not staging"

staging:
		dotenv -f .env.staging run -- pytest -vv -m staging

clean:
		dotenv -f .env.staging run -- python clean_sf.py

start:
		dotenv -f .env.production run -- python -m omnivore

dashboard-dev:
		@dotenv -f .env.dashboard.dev run -- flask run