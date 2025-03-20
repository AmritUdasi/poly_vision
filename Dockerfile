# Build Stage for supervisord_exporter
FROM golang:1.22 AS build-stage

RUN git clone -b main --single-branch https://github.com/salimd/supervisord_exporter.git \
    && cd supervisord_exporter \
    && CGO_ENABLED=0 go build -o /supervisord_exporter

# Application Stage
FROM python:3.9

# Install required tools and dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    curl \
    nodejs \
    supervisor \
    postgresql-client \
    && curl -sL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python and Node.js tools
RUN pip install --no-cache-dir poetry superfsmon

# Copy the supervisord_exporter binary
COPY --from=build-stage /supervisord_exporter /usr/local/bin/supervisord_exporter
RUN chmod +x /usr/local/bin/supervisord_exporter

# Set the working directory
WORKDIR /app

# Copy application files
COPY pyproject.toml .
COPY prisma /app/prisma
COPY . .
COPY wait-for-it.sh /app/wait-for-it.sh 
RUN chmod +x /app/wait-for-it.sh
ENV POETRY_HTTP_TIMEOUT=60

# Install Python dependencies
RUN poetry config virtualenvs.create false \
    && poetry install 

# Add after copying files and before running the application
RUN python -m prisma generate

# Expose the required ports
EXPOSE 5000

# Set the default command
CMD ./wait-for-it.sh \
    "$POSTGRES_SEEDS" \
    "$POSTGRES_USER" \
    "$DB_PORT" \
    "$POSTGRES_DB" \
    "$POSTGRES_PASSWORD" \
    -- python3 -m prisma migrate deploy && \
    /usr/bin/supervisord -c /app/poly_vision/temporal/supervisor/dev.conf
