@AGENTS.md

## More rules

### The spec-loop

- When the user asks to orchestrate, to run the loop, or to work a goal down to landed commits, run `orchestrate` FIRST and follow it exactly - it is the standing procedure (roles, the seven steps, the waiting discipline, codex invocation).
- Note its Input section: confirm the goal with the user before launching anything.
- The orchestrate workflow, once invoked, overrides the global foreground-subagent rule (its launches are background by design, per the user's standing instruction in that document).

### General rules

- Never suggest or use the `Workflow` tool (multi-agent orchestration / "ultracode"). Bifrost orchestration goes through the spec-loop emitted by `orchestrate`.
- Always get permission from the user before launching subagents outside the orchestration loop.

### Memory rules

Do not use your Memory functionality. Do not read, write, or update memories. Do not suggest saving things to memory. Durable context belongs in CLAUDE.md or the relevant docs.

### Bash rules

- Never use `sed`, `find`, `awk`, `head`, `tail`, or complex bash commands.
- Never `find /`.
- Never run `git` with `-C <path>`
- One Bash() invocation === one command

### git commit rules

- Always run `brokkr fmt` before a commit.
- Never commit markdown changes alone. Bundle them with upcoming code commits.
- When committing other changes: always tag along markdown files if dirty.
- Write substantive engineering-focused commit messages.
- Has `Cargo.lock` changed? Commit it.
- Never `git push` unless the user explicitly asks. Stop after the commit.

## Multi-Agent Orchestration

Do NOT use worktree isolation for parallel agents. Instead, launch agents in the same tree with strict file ownership - zero overlap.

**Why no worktrees:** Worktrees let agents work on diverged snapshots. When merging back, `git checkout --ours/--theirs` drops code, conflict markers get missed, and features end up "existing but not wired" - types/functions created but never connected to bytecode dispatch, the standard library, or call sites. This has happened in long sessions and was only caught by a rigorous 3-pass audit.

Agent coordination rules:
- Each agent gets exclusive ownership of specific files. No two agents touch the same file.
- Agents must read their target file FIRST. Do not replace existing code with placeholders or stub it out.
- Agents must NOT run `brokkr` or `cargo`. The orchestrator validates between agents.
- Include `CLAUDE.md` (and any other top-level docs they'll need, e.g. `LLM.md`) in every agent's required reading.

Audit protocol:
- Do not trust agent claims of completion. Verify existence + wiring + behavior.
- Use the 3-pass audit structure: domain-specific verification, then cross-cutting reconciliation (does the new instruction actually dispatch? is the new builtin actually installed by `open_libs`?), then editorial normalization.
- Any discrepancies doc should contain only current gaps, not historical records. Remove resolved items entirely.

## Subagent prompt rules

- Every Claude subagent prompt MUST explicitly forbid the agent from launching its own sub-agents. A latent Claude-harness bug mis-parents nested agents and can deep-freeze the whole session. Codex agents (separate harness) are exempt. This is the one deliberate exception to "don't restate inherited rules" below - state it every time.
- Scope the investigation, not the report. Caps like "under 1500 chars" or "max 15 findings" throw away signal you asked them to surface.
- Invite lateral findings up front. If they notice a bug, optimization, smell, or anything surprising while doing the scoped work, they should flag it, even when it's outside the immediate task.
- Name the question, not the method. Don't prescribe tools ("use `git diff`", "use `Read`"), don't prescribe steps ("read in full, not just hunks"), don't enumerate files when the scope already implies them ("piners-syntax crate only" + the agent's own `ls` / `git diff --name-only` is enough). Prescribing the method wastes tokens and signals distrust.
- Don't restate rules the agent already inherits. Subagents load the same CLAUDE.md / AGENTS.md as the main session, so the bash rules, no-cargo, no-worktrees, gremlins, etc. are already in scope. Re-listing them is noise.
- Do pass anything learned in *this* conversation that the agent can't see: the user's framing, prior decisions, what's already been ruled out, the specific claim being audited.