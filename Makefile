.PHONY: build

MODULE:=git_cdn
PIP:=/usr/bin/env python3 -m pip
POETRY:=/usr/bin/env python3 -m poetry
PIP_VERSION:="==22.0.3"
POETRY_VERSION:="==1.1.12"

# poetry enforce semver PEP 440 https://www.python.org/dev/peps/pep-0440/#local-version-identifiers
# So convert v1.7.1-55-af3454 to v1.7.1+55.af3454
GITCDN_VERSION := $$(git describe --tags HEAD | sed s/\-/\+/ | sed s/\-/\./)
GITCDN_LOCALCHANGE := $$(if [ "$$(git status -s -uno)" ]; then echo ".dirty"; fi)

all: dev style checks test
style: isort black
dev: poetry-install install
sc: style checks
sct: style checks test
checks: isort-check black-check flake8

poetry-install:
	@$(PIP) install --user --upgrade "pip$(PIP_VERSION)" "poetry$(POETRY_VERSION)"
	@$(POETRY) run pip install "pip$(PIP_VERSION)"

git-config:
	git config --global uploadpack.allowfilter true

install:
	@$(POETRY) install

export:
	@$(POETRY) export --without-hashes --output requirements.txt

build:
	@$(POETRY) build

publish:
	@$(POETRY) publish

install-no-dev:
	@$(POETRY) install --no-dev

isort:
	@$(POETRY) run isort -rc $(MODULE)

isort-check:
	@$(POETRY) run isort -rc -c $(MODULE)

black:
	@$(POETRY) run black $(MODULE)

black-check:
	@$(POETRY) run black --check $(MODULE)

flake8:
	$(POETRY) run flake8 --config .flake8 $(MODULE)

pylint:
	@$(POETRY) run pylint --rcfile=.pylintrc --output-format=colorized $(MODULE)

set-version:
	$(POETRY) version $(GITCDN_VERSION)$(GITCDN_LOCALCHANGE)

test: git-config
	@$(POETRY) run pytest --strict $(MODULE)

integration-test: git-config
	@$(POETRY) run pytest --strict git_cdn/tests/test_integ.py

test-v: git-config
	@$(POETRY) run pytest --strict -svv --durations=30 $(MODULE)

test-coverage: git-config
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

run: git-config
	. ./tosource && \
	GUNICORN_WORKER=1 \
	$(POETRY) run gunicorn -c config.py git_cdn.app:app -b :8000

run-uvloop: git-config
	. ./tosource && \
	GUNICORN_WORKER=1 \
	GUNICORN_WORKER_CLASS=aiohttp.worker.GunicornUVLoopWebWorker \
	$(POETRY) run gunicorn -c config.py git_cdn.app:app -b :8000

# aliases to gracefully handle typos on poor dev's terminal
check: checks
devel: dev
develop: dev
styles: style
test-unit: test
unit-tests: test
ut: test

