lint:
	uv run ruff check --select I memcachio tests setup.py
	uv run ruff check memcachio tests setup.py
	uv run ruff format --check memcachio tests setup.py
	uv run mypy memcachio

lint-fix:
	uv run ruff check --select I --fix memcachio tests setup.py
	uv run ruff check --fix memcachio tests setup.py
	uv run ruff format memcachio tests setup.py
	uv run mypy memcachio
