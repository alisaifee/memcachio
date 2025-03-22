lint:
	ruff check --select I memcachio tests setup.py
	ruff check memcachio tests setup.py
	ruff format --check memcachio tests setup.py
	mypy memcachio

lint-fix:
	ruff check --select I --fix memcachio tests setup.py
	ruff check --fix memcachio tests setup.py
	ruff format memcachio tests setup.py
	mypy memcachio
