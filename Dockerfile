FROM ubuntu:22.04
SHELL ["/bin/bash", "-xc"]
ENV DEBIAN_FRONTEND=noninteractive



RUN : && \
    sed -i 's/archive.ubuntu.com/mirrors.tencent.com/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.tencent.com/g' /etc/apt/sources.list && \
    apt-get update && apt-get -y upgrade && \
    apt-get install -y locales python3 python3-distutils git curl gzip libffi-dev libssl-dev && \
    locale-gen zh_CN.UTF-8 && \
    python3 <(curl https://bootstrap.pypa.io/get-pip.py) && \
    pip3 install -U pip && \
    pip3 install -U poetry && \
    poetry config virtualenvs.create false && \
    git config --global pack.threads 4 && \
    git config --global uploadpack.allowfilter true && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    :

ENV TERM xterm
ENV LANG zh_CN.UTF-8
ENV LANGUAGE zh_CN:en
ENV LC_ALL zh_CN.UTF-8

ADD poetry.lock /app/poetry.lock
ADD pyproject.toml /app/pyproject.toml

WORKDIR /app
RUN poetry install --no-dev --no-interaction

ADD git_cdn /app/git_cdn
ADD config.py /app/config.py

ENTRYPOINT ["gunicorn", "git_cdn.app:app", "-c", "config.py"]
# CMD holds the optional arguments (change at will)
CMD ["--bind", ":8000"]
EXPOSE 8000

