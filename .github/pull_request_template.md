# Pull request

## Summary

Describe the narrow behavior change and the safety invariant it affects.

## Evidence

- [ ] Uses only synthetic fixtures
- [ ] Includes a behavioral regression test
- [ ] Preserves rollout bytes
- [ ] Preserves unrelated SQLite rows
- [ ] Keeps unknown inputs fail-closed

## Verification

```text
python3 -B -m unittest discover -s tests -v
ruff format --check .
ruff check .
```
