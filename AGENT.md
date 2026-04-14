# Fortune Bot Local Agent Rules

This file defines project-local agent behavior for `C:\Users\ROG\Desktop\fortune_bot`.

## Command Execution

- Prefer `rtk` for shell command execution in this repository.
- Unless a command cannot be run through `rtk`, use the `rtk` prefix by default.
- Examples:
  - `rtk git status`
  - `rtk pytest -q`
  - `rtk rg "OrderRequest" .`
  - `rtk python -m execution_engine`

## Scope

- Keep all agent configuration local to this repository.
- Do not modify global Codex, Gemini, shell, or editor configuration from this project.
- Do not write to `~/.codex`, `~/.gemini`, or other global config locations for this repo workflow.

## Workflow

- Follow the repository workflow documented in `AGENTS.md`.
- Treat `AGENTS.md` as the primary project policy document.
- Treat `RTK.md` as the command execution rule for this repository.

## Safety

- Treat `fortune_bot.env` and any secrets as sensitive.
- Avoid printing, copying, or rewriting secret values.
- Preserve unrelated user changes in the worktree.
