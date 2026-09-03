FROM python:3.14-slim

WORKDIR /opt/project

# Dependencies are installed from the same pinned files CI uses, so the container and the test
# environment cannot drift. requirements-api.txt adds the pieces only a running server needs
# (the Trino driver and uvicorn); requirements-ci.txt supplies fastapi/pydantic.
COPY requirements-ci.txt requirements-api.txt ./
RUN pip install --no-cache-dir -r requirements-ci.txt -r requirements-api.txt

COPY api/ ./api/

# curl backs the container healthcheck; the readiness endpoint it hits requires Trino.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8000

CMD ["uvicorn", "api.src.app:app", "--host", "0.0.0.0", "--port", "8000"]
