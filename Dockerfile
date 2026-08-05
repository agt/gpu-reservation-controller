FROM python:3.13-slim AS deps

WORKDIR /app

# Install from the compiled lockfiles, not the loose `requirements*.txt`.  Both
# are lower-bound only, so a plain install resolves whatever is newest at build
# time — and because `final` below reinstalls on a clean base, this stage could
# test one dependency set while the shipped image ran another.  The locks are
# what make the two resolutions identical.
#
# `requirements-dev.lock` is compiled with `requirements.lock` as a constraint,
# so it is a superset at identical versions; `tests/test_dependency_lock.py`
# asserts that rather than trusting it.  Regenerate both together:
#
#   uv pip compile requirements.txt -o requirements.lock
#   uv pip compile requirements-dev.txt -c requirements.lock -o requirements-dev.lock
COPY requirements.txt requirements-dev.txt requirements.lock requirements-dev.lock ./
RUN pip install --no-cache-dir -r requirements-dev.lock


# Runs the full test suite.  A non-zero exit here fails the build, so a broken
# commit cannot produce a deployable image.
#
# **This stage only runs when it is built explicitly** (`--target test`).  It is
# not in the dependency graph of `final` below — nothing COPYs from it — so a
# plain `docker build` prunes it and runs no tests at all.  The CI workflow
# therefore has a dedicated `Run tests` step; keep the two in step, or the suite
# silently stops gating releases.
FROM deps AS test

COPY app/ ./app/
COPY tests/ ./tests/
COPY pytest.ini .
# tests/test_log_grammar.py asserts the log grammar against these two documents,
# so they are part of the test inputs, not just prose.
COPY docs/ ./docs/
COPY OBSERVABILITY.md .
# tests/test_shared_artifacts.py imports the checker that hashes the four files
# kept byte-identical with the sibling repo, so scripts/ is a test input too.
# (All four artifacts themselves are already covered by app/ and docs/ above.)
COPY scripts/ ./scripts/
# tests/test_chart_image.py parses the chart and the workflow (helm is not a test
# dependency, so it checks the values rather than rendering them), which makes
# both deployment config a test input.  Nothing is copied out of this stage, so
# they never reach the runtime image.
COPY helm/ ./helm/
COPY .github/ ./.github/
# tests/test_docker_test_stage.py reads this file to check the COPY list below
# against what the suite actually opens.
COPY Dockerfile .

# tests/test_docker_test_stage.py guards the list above: a test that reads a
# repo-root path this stage does not COPY passes locally and errors here, where
# the file simply is not present.
RUN pytest --tb=short -q


FROM python:3.13-slim AS final

WORKDIR /app

# The same lock the `deps`/`test` stages installed, so what shipped is what was
# tested.  `final` deliberately reinstalls on a clean base rather than copying
# site-packages from `deps`, which is what keeps the dev-only packages out of
# the runtime image.
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock

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
        "import os,urllib.request; urllib.request.urlopen('http://localhost:%s/health' % os.environ.get('HTTP_PORT','8000'))" \
    || exit 1

# Launch via the module entrypoint so HTTP_PORT controls the bind port
# (uvicorn is started programmatically from app.main:main).
CMD ["python", "-m", "app.main"]
