# Agent Rules (Aider / Coding Agent Contract)

You are an autonomous coding agent working inside this repository.
You are allowed to modify files in this repo, create new files, and run commands,
as long as you follow the rules below.

- When I ask you to do something (commits, tests, formatting, pushing), you must run the required shell commands and report the outputs. Do not only provide instructions.
- Before proposing commit splits, you must run: `git status -sb`, `git diff --stat`, and `git diff` (or `git diff --name-only`) to ground the plan in reality.

## 0) Operating Principles
- Prefer small, reviewable changes over big refactors.
- Make progress fast, but never at the cost of breaking the project.
- When uncertain, choose the safest path and explain assumptions.
- Keep code, comments, docstrings, and documentation in **English only**.

## 1) Default Workflow (Always Follow)
Before making changes:
1) Identify the goal in one sentence.
2) Read only the files you need (start small).
3) Propose a plan in 5–10 bullets.
4) Confirm the plan **only if** it includes risky changes (see Section 2).

When implementing:
5) Edit files with minimal diffs.
6) Keep changes scoped to the goal.
7) Add/adjust tests when behavior changes.
8) Run verification commands (Section 4) and report results.

When done:
9) Summarize: what changed, why, and how to verify.
10) Provide a clean file list of modified/added files.

## 2) What You Can Do Without Asking
You may proceed without asking for approval when:
- The change is small and localized (one feature, one bugfix).
- It does not introduce new dependencies.
- It does not touch secrets, auth, billing, or production deployment.

You must ask before:
- Adding/removing dependencies (Python or npm).
- Large refactors affecting multiple modules.
- Anything that changes API contracts, data schemas, or file formats.
- Changes involving credentials, tokens, `.env`, authentication, rate limits, or security policies.
- Deleting files or performing irreversible migrations.

## 3) File Editing Rules
- Prefer existing patterns and style in the repo.
- Avoid "cleanup refactors" unrelated to the task.
- Keep functions small, names clear, and logging meaningful.
- Add type hints in Python when reasonable.
- For JavaScript/TypeScript, keep code consistent with existing lint/format rules.

## 4) Verification Commands (Run as Appropriate)
### Python (this repo uses pyproject + requirements)
Preferred:
- `python -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`
- `pip install -r requirements-dev.txt` (if needed)
- `pytest -q`

If formatting/lint tools exist, run:
- `ruff check .` (if present)
- `ruff format .` (if present)

### Web / Node (only if web folder is part of the task)
- `npm ci` (or `npm install` if no lockfile)
- `npm run lint` (if present)
- `npm test` (if present)
- `npm run build` (if present)

Rule:
- If a command fails, do not guess wildly. Read the error, locate the root cause, and fix it with minimal changes.

## 5) Testing Rules
- If you change behavior, update or add tests.
- Prefer unit tests over integration tests unless integration is required.
- Keep tests deterministic (no real network calls unless explicitly requested).
- If external APIs are involved (e.g., TDX), mock the network calls.

## 6) Logging & Error Handling
- Fail gracefully and return actionable error messages.
- Use structured logs when available.
- Avoid swallowing exceptions without logging.

## 7) Secrets & Safety
- Never print or commit secrets (API keys, tokens, credentials).
- Do not modify `.env` with real secrets.
- If a secret is required, request that the user provide it via environment variables.
- Ensure `.env` stays gitignored.

## 8) Git Discipline (If git is available)
- Keep commits small and descriptive.
- Suggest Conventional Commits messages (e.g., `fix: ...`, `feat: ...`, `chore: ...`).
- Do not force-push or rewrite history unless explicitly requested.

## 9) Output Format (Every Response)
Provide:
- Plan (bullets)
- Files changed (list)
- Commands run (list)
- Results (what passed/failed)
- Next steps (if any)

## 10) If You Need More Context
- Ask to add specific files into context (max 5 at a time).
- Prefer: README, config files, and the smallest relevant module first.
