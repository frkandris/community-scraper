FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir .

COPY scraper/ ./scraper/
COPY config/   ./config/

# Embed build timestamp so the version string works without git history in the container
RUN date -u '+%Y-%m-%d.%H:%M' > /app/VERSION

# data/ is mounted as a persistent volume at /app/data
# The full /app directory is used as the git working tree

EXPOSE 8000

CMD ["python", "-m", "scraper.main"]
