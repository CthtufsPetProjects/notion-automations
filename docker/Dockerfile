# Use the official Python image from the Docker Hub, specifying Python 3.12
FROM python:3.12-slim

# Set environment variables for Python and Poetry
ENV PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.8.3 \
    POETRY_VIRTUALENVS_CREATE=false

# Install Poetry
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get remove -y curl && apt-get autoremove -y

# Set the working directory inside the container
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files to the working directory
COPY pyproject.toml poetry.lock* ./

# Install dependencies using Poetry
RUN ~/.local/share/pypoetry/venv/bin/poetry install --no-root --no-interaction --no-ansi

# Copy the rest of the application code into the container
COPY . .

ENTRYPOINT ["/app/docker/entrypoint.sh"]
