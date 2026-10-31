FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# Runtime and test dependencies are locked; see docs/testing.md for the scope of
# reproducibility (the base-image tag is not a platform-specific digest lock).
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock
COPY pyproject.toml README.md ./
COPY src ./src
COPY examples ./examples
RUN python -m pip install --no-cache-dir --no-deps --no-build-isolation . \
    && useradd --uid 10001 --create-home gateway
USER gateway
EXPOSE 8080
CMD ["mcp-gateway", "--config", "/app/examples/gateway-a.json", "--host", "0.0.0.0", "--port", "8080"]
