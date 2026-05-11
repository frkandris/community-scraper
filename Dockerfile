FROM python:3.12-slim

WORKDIR /app

ENV HOST=0.0.0.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY scraper/ ./scraper/
COPY config/   ./config/
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

CMD ["python", "-m", "scraper.main"]
