FROM python:3.8-slim

ARG USER=xtdb-user

RUN groupadd --gid 1000 $USER && adduser --disabled-password --gecos '' --uid 1000 --gid 1000 $USER
USER $USER

WORKDIR /home/$USER/xtdb-py

ENV PATH="/home/$USER/.local/bin:/home/$USER/xtdb-py/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true

RUN --mount=type=cache,target=/root/.cache pip install --user poetry==1.2.0

COPY --chown=$USER:$USER ./pyproject.toml ./poetry.lock ./
RUN --mount=type=cache,target=/root/.cache poetry install --with dev --no-root

COPY --chown=$USER:$USER ./ ./
RUN --mount=type=cache,target=/root/.cache poetry install
