ruff:
	uv run ruff format
	uv run ruff check --fix

ty:
    uv run ty check src
