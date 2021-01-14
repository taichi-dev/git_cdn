FROM python:3.8-alpine

WORKDIR     /app

# Only install dependencies
RUN  apk --no-cache add make git libstdc++ && \
    apk add --update --no-cache libffi curl openssl

ADD dist/*.whl /app/
RUN apk add --update --no-cache --virtual .build-deps alpine-sdk musl-dev libffi-dev openssl-dev &&\
    python -m pip install /app/*.whl && \
    apk del .build-deps && \
# Configure git for git-cdn
    git config --global pack.threads 4

ADD config.py /app/

# entrypoint contains stuff that you shouldn't want to customize
# starts gunicorn
ENTRYPOINT ["gunicorn", "git_cdn.app:app", "-c", "config.py"]
# CMD holds the optional arguments (change at will)
CMD ["--workers", "8", "--bind", ":8000"]
EXPOSE 8000
