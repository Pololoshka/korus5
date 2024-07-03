# Setup base
FROM apache/airflow:2.9.2-python3.12 AS base

# ENV for python and poetry will be shared accross stages
ENV PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  \
  # pip
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  \
  # poetry
  # https://python-poetry.org/docs/configuration/#using-environment-variables
  POETRY_VERSION=1.8.2 \
  # make poetry install to this location
  POETRY_HOME="/opt/poetry" \
  # make poetry create the virtual environment in the project's root
  # it gets named `.venv`
  POETRY_VIRTUALENVS_IN_PROJECT=true \
  # do not ask any interactive question
  POETRY_NO_INTERACTION=1 \
  \
  # paths this is where our requirements + virtual environment will live
  PYSETUP_PATH="/opt/pysetup" \
  VENV_PATH="/opt/pysetup/.venv"

# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"


# ###########
# BUILD STAGE
# ###########

# Setup poetry and python dependencies
FROM base AS build-stage

USER root

# deps for installing poetry and for building python deps
RUN apt-get update \
  && apt-get install --no-install-recommends -y \
  curl \
  build-essential

# install poetry - respects $POETRY_VERSION & $POETRY_HOME
# The --mount will mount the buildx cache directory to where
# Poetry and Pip store their cache so that they can re-use it
RUN --mount=type=cache,target=/root/.cache \
  curl -sSL https://install.python-poetry.org | python3 -

# copy project requirement files here to ensure they will be cached.
WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./
# install runtime deps - uses $POETRY_VIRTUALENVS_IN_PROJECT internally
RUN poetry install --without=dev


################################
# PRODUCTION
################################
FROM base AS production
COPY --from=build-stage $PYSETUP_PATH $PYSETUP_PATH
