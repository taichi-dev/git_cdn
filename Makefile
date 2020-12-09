.PHONY: build

MODULE:=git_cdn

all: dev style checks dists test

dev: install-pipenv pipenv-install-dev requirements

install-pipenv:
	pip3 install --user --upgrade 'pipenv>=9.0' 'pip>=9.0'
	@echo "ensure your local python install is in your PATH"

pipenv-install-dev:
	pipenv install --dev --python 3.7
	pipenv run pip install -e .

install-local: install-local-only-deps install-local-only-curpackage

install-local-only-deps:
	# Install only dependencies
	pipenv install

install-local-only-curpackage:
	# Install current package as well
	pipenv run pip install .

style: isort black

isort:
	pipenv run isort .

isort-check:
	pipenv run isort . --check

black:
	pipenv run black $(MODULE)

black-check:
	pipenv run black --check $(MODULE)

checks: sdist isort-check black-check flake8 pylint

flake8:
	pipenv run python setup.py flake8

pylint:
	pipenv run pylint --rcfile=.pylintrc --output-format=colorized $(MODULE)

sc: style check

sct: style check test

build: dists

shell:
	pipenv shell

GITCDN_VERSION := $$(git describe --tags HEAD)
GITCDN_LOCALCHANGE := $$(if [ "$$(git status -s -uno)" ]; then echo "~"; fi)
VERSION_FILE := git_cdn/version.py

VERSION_FILE: set-version

set-version:
	@echo "GITCDN_VERSION = '$(GITCDN_VERSION)$(GITCDN_LOCALCHANGE)'" > $(VERSION_FILE)

clean-version:
	rm -f $(VERSION_FILE)


test:
	pipenv run pytest --strict $(MODULE)

integration-test:
	pipenv run pytest --strict git_cdn/tests/test_integ.py

test-v:
	pipenv run pytest --strict -svv --durations=30 $(MODULE)

test-coverage:
	pipenv run pytest --junitxml=testresults.xml -v --cov --cov-report term-missing --cov-report html:`pwd`/coverage_html  $(MODULE)

requirements:
	# needed until PBR supports `Pipfile`
	pipenv run pipenv_to_requirements

dists: requirements sdist bdist wheels

sdist:
	pipenv run python setup.py sdist

bdist:
	pipenv run python setup.py bdist

wheel:
	pipenv run python setup.py bdist_wheel
docker:
	docker build .

ctags:
	find -name '*.py' -exec ctags -a {} \;

update:
	pipenv update --clear

update-recreate: update dev style check


lock:
	pipenv lock

githook: style requirements

push: githook
	git push origin --all
	git push origin --tags

publish: clean-dist dists
	find dist/ -name "*.gz" -or -name "*.whl" -print0 | xargs -0 -n1 twine upload -r nexus --skip-existing || true


clean-dist:
	rm -rfv build dist/

clean: clean-dist
	pipenv --rm || true
	find . -name '__pycache__'  -exec rm -rf {} \; || true
	find . -name '.cache'  -exec rm -rf {} \; || true
	find . -name "*.pyc" -exec rm -f {} \; || true
	rm -rf .venv .mypy_cache || true
	rm -rf .venv nestor/testenv || true

run: VERSION_FILE
	. ./tosource && \
	pipenv run gunicorn -c config.py git_cdn.app:app --workers=4


# aliases to gracefully handle typos on poor dev's terminal
check: checks
devel: dev
develop: dev
dist: dists
styles: style
test-unit: test
unittest: unit
unittests: unit
unit-tests: test
ut: test
wheels: wheel
