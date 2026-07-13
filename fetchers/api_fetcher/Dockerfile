# Build context must be ./fetchers/ (not this subdirectory) so that the
# shared/ package can be COPY-ed alongside api_fetcher/.
#
# In docker-compose.dev.yml:
#   build:
#     context: ./fetchers
#     dockerfile: api_fetcher/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer-cached unless requirements change)
COPY api_fetcher/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared package and the fetcher itself
COPY shared/     ./shared/
COPY api_fetcher/ ./api_fetcher/

# PYTHONPATH lets "from shared.xxx import ..." resolve to /app/shared
ENV PYTHONPATH=/app

WORKDIR /app/api_fetcher

CMD ["python", "main.py"]
