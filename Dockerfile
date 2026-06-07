FROM python:3.13-slim

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

# Optional tuning — override as needed.
ENV RESERVATION_FETCH_INTERVAL="300"
ENV RESERVATION_LOOKAHEAD_DAYS="7"
ENV HEALTH_PORT="8000"

# KUBECONFIG is intentionally unset; the controller uses in-cluster credentials
# by default.  Set KUBECONFIG to a mounted kubeconfig path for out-of-cluster use.

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" \
    || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
