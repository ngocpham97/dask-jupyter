FROM python:3.10-slim

RUN apt-get update \
    && apt-get install -y \
        git \
        bash-completion \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV HOME=/tmp

WORKDIR /jupyter-runner

COPY . .

RUN pip install -r requirements.txt

RUN pip install ipykernel

RUN chmod -R 777 /jupyter-runner