FROM python:3.13-slim AS deps

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt


FROM deps AS test

COPY app/ ./app/
COPY tests/ ./tests/
COPY pytest.ini .

RUN pytest --tb=short -q


FROM python:3.13-slim AS final

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

RUN adduser --disabled-password --gecos "" --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Required — must be supplied at deploy time (mount from a Kubernetes Secret).
ENV RESERVATION_API_URL=""
ENV RESERVATION_API_KEY=""

# Optional tuning — a representative subset shown here for visibility; these
# match the application defaults in config.py.  The full set of tunables is
# documented in README.md and CLAUDE.md and can be overridden at deploy time
# (the Helm chart wires them all).
ENV RESERVATION_FETCH_INTERVAL="300"
ENV RESERVATION_LOOKAHEAD_DAYS="7"
ENV HTTP_PORT="8000"

# KUBECONFIG is intentionally unset; the controller uses in-cluster credentials
# by default.  Set KUBECONFIG to a mounted kubeconfig path for out-of-cluster use.

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c \
        "import os,urllib.request; urllib.request.urlopen('http://localhost:%s/health' % (os.environ.get('HTTP_PORT') or os.environ.get('HEALTH_PORT','8000')))" \
    || exit 1

# Launch via the module entrypoint so HTTP_PORT controls the bind port
# (uvicorn is started programmatically from app.main:main).
CMD ["python", "-m", "app.main"]
