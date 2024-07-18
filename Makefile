make fix:
	ruff format .
	ruff check --fix .
	sqlfluff fix .
