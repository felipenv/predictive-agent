FROM python:3.10-slim

COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /uvx /bin/

ENV GIT_PYTHON_REFRESH=quiet

WORKDIR /app

COPY dockerfiles/requirements.txt .

RUN uv pip install --system -r requirements.txt
RUN uv pip install --system protobuf==3.20.3

