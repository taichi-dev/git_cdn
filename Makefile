.PHONY: build

MODULE:=git_cdn
PIP:=/usr/bin/env python3 -m pip
POETRY:=/usr/bin/env python3 -m poetry
PIP_VERSION:="==20.3.3"
POETRY_VERSION:="==1.1.0"

GITCDN_VERSION := $$(git describe --tags HEAD)
GITCDN_LOCALCHANGE := $$(if [ "$$(git status -s -uno)" ]; then echo "~"; fi)
VERSION_FILE := git_cdn/version.py

all: dev style checks dists test
style: isort black
dev: poetry-install install
sc: style checks
sct: style checks test
checks: isort-check black-check

poetry-install:
	@$(PIP) install --user --upgrade "pip$(PIP_VERSION)" "poetry$(POETRY_VERSION)"
	@$(POETRY) run pip install "pip$(PIP_VERSION)"

install:
	@$(POETRY) install

build:
	@$(POETRY) build

publish:
	@$(POETRY) publish

install-no-dev:
	@$(POETRY) install --no-dev

isort:
	@$(POETRY) run isort -rc .

isort-check:
	@$(POETRY) run isort -rc -c .

black:
	@$(POETRY) run black .

black-check:
	@$(POETRY) run black --check .

pylint:
	@$(POETRY) run pylint --rcfile=.pylintrc --output-format=colorized $(MODULE)

VERSION_FILE: set-version

set-version:
	@echo "GITCDN_VERSION = '$(GITCDN_VERSION)$(GITCDN_LOCALCHANGE)'" > $(VERSION_FILE)
	$(POETRY) version $(VERSION)

clean-version:
	rm -f $(VERSION_FILE)


test:
	@$(POETRY) run pytest --strict $(MODULE)

integration-test:
	@$(POETRY) run pytest --strict git_cdn/tests/test_integ.py

test-v:
	@$(POETRY) run pytest --strict -svv --durations=30 $(MODULE)

test-coverage:
	@$(POETRY) run pytest --junitxml=testresults.xml -v --cov --cov-report term-missing --cov-report html:`pwd`/coverage_html  $(MODULE)

docker:
	docker build .

ctags:
	find -name '*.py' -exec ctags -a {} \;

update:
	@$(POETRY) update

lock:
	@$(POETRY) lock

githook: style

push: githook
	git push origin --all
	git push origin --tags

run: VERSION_FILE
	. ./tosource && \
	@$(POETRY) run gunicorn -c config.py git_cdn.app:app --workers=4


# aliases to gracefully handle typos on poor dev's terminal
check: checks
devel: dev
develop: dev
styles: style
test-unit: test
unittest: unit
unittests: unit
unit-tests: test
ut: test

