FROM python:3.12-slim

WORKDIR /app

ENV HOST=0.0.0.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY scraper/ ./scraper/
COPY config/   ./config/
# Maintenance scripts (provider scoring, city import) must be runnable in the
# container: that is where the database and the API keys live.
COPY scripts/  ./scripts/
COPY tailwind.config.js .

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir . && \
    playwright install chromium --with-deps

# Build Tailwind CSS (pytailwindcss uses the standalone binary, no Node needed)
RUN pip install --no-cache-dir pytailwindcss && \
    tailwindcss -i ./scraper/web/static/css/input.css \
                -o ./scraper/web/static/css/app.css \
                --minify && \
    pip uninstall -y pytailwindcss

# Embed build timestamp so the version string works without git history
RUN TZ=Europe/Budapest date '+%Y-%m-%d.%H:%M' > /app/VERSION

# data/ and config/ can be mounted as persistent volumes.

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')" || exit 1

CMD ["python", "-m", "scraper.main"]
