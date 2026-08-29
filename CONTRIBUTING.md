# Contributing

This project repairs local state that may contain sensitive history, so safety
and reproducibility take priority over broad format support.

## Before proposing a change

1. Describe the observable resume failure and the exact local evidence that
   distinguishes durable rollout data from the derived projection.
2. Reduce the case to a synthetic fixture. Never submit a real rollout, SQLite
   database, prompt, tool output, absolute user path, or thread identifier.
3. State why an existing fail-closed boundary is insufficient.
4. Add a behavioral test that fails before the change and passes afterward.

## Development checks

```bash
python3 -B -m unittest discover -s tests -v
ruff format --check .
ruff check .
```

The runtime must remain standard-library only. Development tooling is declared
in `pyproject.toml`.

## Pull requests

- Keep one concern per pull request.
- Explain the safety invariant affected by the change.
- Include before/after structural evidence using synthetic data.
- Confirm that raw rollout bytes remain unchanged.
- Confirm that unknown inputs still fail closed.
- Do not include generated caches or recovery backups.

Use Conventional Commits for commit messages.

## License

By submitting a contribution, you agree that it is licensed under the
[MIT License](LICENSE).
