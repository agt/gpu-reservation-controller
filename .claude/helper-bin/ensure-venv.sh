#!/bin/bash

[ -d "$CLAUDE_PROJECT_DIR" ] || exit 1
cd "${CLAUDE_PROJECT_DIR}" || exit 1

VENV_DIRNAME=".venv"

if [ \! -d "${VENV}" ]; then
	uv venv "${VENV_DIRNAME}"
fi

cat > "${CLAUDE_ENV_FILE}" << "EOM
export VIRTUAL_ENV="${CLAUDE_PROJECT_DIR}/${VENV_DIRNAME}"
export PATH="${CLAUDE_PROJECT_DIR}/${VENV_DIRNAME}/bin:$PATH"
EOM

