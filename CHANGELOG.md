# Changelog

All notable changes to `awrise` are recorded here.

## [0.5.0] - 2026-11-08

### Added

- **awrise**: every bad wake says WHY. Its ledger row carries `why` (the job's own most informative last error line), a `cause` bucket (`dependency-down`, `code-bug`, `timeout`, `config`, `unknown`) and `output_tail` (stdout+stderr, last 40 lines, at most 4 KB, redacted). A detached job's why is read from its receipt and the log it names. `status`, `history` and `explain` show it; rows written before this answer from their old tails.
- **awrise**: a dependency that is down is HELD, not piled up. The same dependency-down why 3 times in a row skips the job (`skipped_held`, one policy row per window) for two windows, doubling per repeat, capped at 1 h; a success clears it.
- **awrise**: the same code-bug why 5 times raises ONE `wake-failed` decision card per (job, why streak), carrying the why and the tail. Its `fix` option runs `awrise investigate --name <job> --dispatch`.
- **awrise**: `investigate --name N [--dispatch]` prints a fix agent's brief, and with `--dispatch` starts the agent on the harness daemon.
- **awrise**: redaction also masks JWTs, Stripe live keys, and `token=`/`password:`/`api_key=`-style values.

## [0.4.3] - 2026-11-07

### Fixed

- **awrise**: on Linux a zombie (exited, not yet reaped) counts as dead, so an overdue detached child that was SIGKILLed no longer keeps its job's lock Held.
- **awrise**: `__version__` and the package version are one number again.

