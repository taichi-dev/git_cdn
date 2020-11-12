FROM python:3.7-alpine

WORKDIR     /app
ADD         Pipfile* Makefile /app/
# Disable PBR try to read version from git information for modules in the deps/ directory
# If PBR cannot find metadata information (only for sdist/bdist/wheel packages), it will try
# to find the version according to the git tree, and so try to connect to upstream remote.
# In the case of docker, the upstream is unavailable, thus making PBR failing for no reason,
# since the versionning is actually handled by the git submodule mechanism.
ARG         PBR_VERSION=0.0.1
# Only install dependencies
RUN  apk --no-cache add make git libstdc++ && \
    apk add --update --no-cache --virtual .build-deps alpine-sdk musl-dev libffi-dev curl &&\
    pip install -U pip pipenv && \
    make install-local-only-deps && \
    apk --purge del .build-deps
ADD         . /app/

# Only install current package
RUN         make install-local-only-curpackage

# Configure git for git-cdn
RUN git config --global pack.threads 4

# entrypoint contains stuff that you shouldn't want to customize
# starts gunicorn
ENTRYPOINT [ "pipenv", "run", "gunicorn", "git_cdn.app:app", "-c", "config.py"]
# CMD holds the optional arguments (change at will)
CMD ["--workers", "8", "--bind", ":8000"]
EXPOSE 8000
