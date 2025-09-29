lint:
	uv run ruff check --select I memcachio tests
	uv run ruff check memcachio tests
	uv run ruff format --check memcachio tests
	uv run mypy memcachio

lint-fix:
	uv run ruff check --select I --fix memcachio tests
	uv run ruff check --fix memcachio tests
	uv run ruff format memcachio tests
	uv run mypy memcachio
