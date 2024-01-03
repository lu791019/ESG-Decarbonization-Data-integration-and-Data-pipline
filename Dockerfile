FROM python:3.9-bullseye

RUN pip install poetry

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./

# Install the dependencies
RUN poetry export --with=dev --without-hashes -o requirements.txt
RUN pip install -r requirements.txt

# Copy the application code into the container
COPY . .

ENV FLASK_APP=wsgi.py

EXPOSE 5002

# Set the command to run the Flask application
CMD ["flask", "run", "--port=5002", "--host=0.0.0.0"]