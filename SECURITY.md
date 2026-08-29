# Security policy

## Sensitive artifacts

Codex rollout files and SQLite stores may contain prompts, source code, command
output, local paths, account context, and other private data. Never attach raw
rollouts, databases, backup bundles, credentials, screenshots of private
history, or complete logs to an issue or pull request.

When reporting a problem, provide only:

- Codex version and operating system;
- helper status and blocker names;
- cursor offsets and ordinals;
- record types at the boundary;
- aggregate line, turn, item, and byte counts;
- hashes only when they do not identify a shared private artifact.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting when it is available. Otherwise,
open a sanitized issue requesting private contact without including exploit
details or sensitive artifacts. Never place an unreviewed state-mutation recipe
in a public issue.

## Supported versions

Until the first release, only the current `main` branch receives security and
correctness fixes.
