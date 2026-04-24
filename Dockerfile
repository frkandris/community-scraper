FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY scraper/ ./scraper/
COPY config/   ./config/

# data/ is mounted as a persistent volume at /app/data
# The full /app directory is used as the git working tree

EXPOSE 8000

CMD ["python", "-m", "scraper.main"]
