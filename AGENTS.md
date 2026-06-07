
This file documents development standards for AI coding agents (Claude Code, Codex, etc.).  
Read this alongside `CLAUDE.md` for full context.

## IMPORTANT - Intital Project Setup

As a first step before any work in this repository, you _must_ create a local Python Virtual Environment ".venv":

   uv venv .venv && source .venv/bin/activate && test -f requirements.txt && uv pip install -r requirements.txt

An external hook will ensure all shell commands are executed within this Virtual Environment -- but you must create it.

## What not to do

- Do not add `__pycache__`, `*.pyc`, `lab_equipment.db`, or `.secret_key` to version control — they are gitignored.
- Do not install packages with bare `pip install`; always use `uv pip install` inside the activated venv.
