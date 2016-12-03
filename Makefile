export VIRTUAL_ENV?=./venv

PROJ_NAME=amqproto

BIN=$(VIRTUAL_ENV)/bin

PIP=$(BIN)/pip
ISORT=$(BIN)/isort
PYFORMAT=$(BIN)/pyformat
FLAKE8=$(BIN)/flake8
PYLINT=$(BIN)/pylint
PYTEST=$(BIN)/pytest


codestyle-check: codestyle-autoformat
	$(FLAKE8) --count $(PROJ_NAME)
	$(PYLINT) $(PROJ_NAME)
	git diff --exit-code $(PROJ_NAME)
	echo "Your code is perfectly styled, congratz! :)"

codestyle-autoformat: codestyle-deps
	$(ISORT) -p $(PROJ_NAME) -ls -sl -rc $(PROJ_NAME)
	$(PYFORMAT) -r -i $(PROJ_NAME)

codestyle-deps:
	$(PIP) install -r requirements/codestyle.txt

integrationtests: tests-deps
	$(PYTEST) -v -l --cov=$(PROJ_NAME) --cov-report=term-missing:skip-covered tests/integration

unittests: tests-deps
	$(PYTEST) -v -l --cov=$(PROJ_NAME) --cov-report=term-missing:skip-covered tests/unit

tests-deps:
	$(PIP) install -r requirements/test.txt

devtools:
	$(PIP) install -r requirements/dev.txt
