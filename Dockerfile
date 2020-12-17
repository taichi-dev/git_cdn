FROM python:3.7-alpine

WORKDIR     /app
ADD         poetry* pyproject.toml Makefile /app/
# Only install dependencies
RUN  apk --no-cache add make git libstdc++ && \
    apk add --update --no-cache --virtual .build-deps alpine-sdk musl-dev libffi-dev curl openssl-dev &&\
    make poetry-install && \
    make install-no-dev && \
    apk --purge del .build-deps
ADD         . /app/

# Configure git for git-cdn
RUN git config --global pack.threads 4

# entrypoint contains stuff that you shouldn't want to customize
# starts gunicorn
ENTRYPOINT ["python3", "-m", "poetry", "run", "gunicorn", "git_cdn.app:app", "-c", "config.py"]
# CMD holds the optional arguments (change at will)
CMD ["--workers", "8", "--bind", ":8000"]
EXPOSE 8000
