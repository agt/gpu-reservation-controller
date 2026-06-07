## Initial Setup -- IMPORTANT

The first time you interact with this repository, review and follow
initial setup instructions in AGENTS.md.

## Technology choices

The following core technologies should be used unless the user expressly requests otherwise.

### Backend

| Choice | Rationale |
|--------|-----------|
| **FastAPI** | Async-capable, automatic OpenAPI docs, Pydantic validation built in |
| **SQLAlchemy 2 ORM** | Declarative models, relationship loading, portable across databases |
| **SQLite** | Zero-configuration, file-based, sufficient for single-lab concurrency |
| **Argon2id** (argon2-cffi) | OWASP #1 recommendation for new applications; memory-hard, resists GPU/ASIC attacks; parameters: m=19456 MiB, t=2, p=1 |
| **python-jose** | JWT creation and validation; HS256 algorithm; 8-hour token lifetime |

### Frontend

| Choice | Rationale |
|--------|-----------|
| **Vanilla HTML/CSS/JS** | No build step, no bundler, no framework dependency — easy to modify and audit |
| **CSS custom properties** | Consistent theming without a preprocessor |
| **Font Awesome 6 (CDN)** | Icon set without adding a build pipeline |
| **localStorage JWT** | Simple for a same-origin SPA; tokens expire after 8 hours |

## Architecture

The following general architecture should be followed for all apps:

```
demo-web-app/
├── app/                  Python package — FastAPI application
│   ├── main.py           App entry point; mounts routers + static files
│   ├── database.py       SQLAlchemy engine, session factory, Base class
│   ├── models.py         ORM models (User, Equipment, CheckoutPolicy, Checkout)
│   ├── schemas.py        Pydantic request/response models
│   ├── auth.py           Password hashing (Argon2id), JWT creation/validation
│   └── routers/
│       ├── auth_router.py      POST /api/auth/login, GET /api/auth/me
│       └── ...
├── static/               Served as-is by FastAPI StaticFiles at "/"
│   ├── index.html        Redirect shim (→ login or dashboard)
│   ├── login.html
│   ├── css/app.css       Single stylesheet; CSS custom properties for theming
│   └── js/
│       ├── api.js        fetch wrapper, auth helpers, tets
├── seed.py               One-shot database seeding script
├── requirements.txt
├── Dockerfile
├── CLAUDE.md
├── AGENTS.md
└── .gitignore
```

FastAPI serves both the REST API (under `/api/`) and the static frontend from the same process on the same origin, so no CORS configuration is required.

## Configuration

Runtime configuration should be accomplished through environment variables whenever feasible.

## Deployment

Update the initial Dockerfile to reflect application-specific build or deployment steps.

Include an appropriate API health check.


