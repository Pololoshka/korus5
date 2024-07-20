install_pre_commit:
	pre-commit install
	pre-commit install --hook-type commit-msg
	pre-commit autoupdate

make fix:
	ruff format .
	ruff check --fix .
	sqlfluff fix .

make check:
	ruff format --check .
	ruff check .
	mypy .
	sqlfluff lint .
