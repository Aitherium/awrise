# Changelog

All notable changes to `awrise` are recorded here.

## [Unreleased]

### Fixed

- **awrise**: on Windows a shell job whose `run` starts with a bare `bash` now runs Git Bash by absolute path (`AWRISE_GIT_BASH`, then the Git install roots, then the `git` on PATH). cmd.exe resolved that word to `System32\bash.exe` -- WSL's default distro -- which cannot open a `C:/...` script and boots a second distro beside the fleet (issue #17561: `dgx-crash-capture` failed x10 with `/bin/bash: C:/...: No such file or directory`). With no Git Bash the wake is an `error` (`wsl_bash_refused:`), never a WSL spawn. A job that means WSL says `wsl -d <distro> ...`, which is untouched.

## [0.6.1] - 2026-11-16

### Fixed

- **awrise**: the critical lane raises its own process to normal CPU and normal I/O priority before it reads the ledger (`procprio`, Windows). Task Scheduler starts the pass at priority 7 -- below-normal CPU and LOW I/O -- and measured 2026-11-16 the pass read its ledger at 57 KB/s (7 reads a second; the same file reads at 90 MB/s from a normal-priority shell), so the first lane pass took 122 s to reach its first pager. The normal lane puts itself back to what the scheduler gave it, so the desktop sees no new load. Each `tick` row carries `io_priority`.
- **awrise**: `reconcile` reads the ledger days that can still hold an open wake (the longest job timeout plus two days, at least 3) instead of a flat 30. Thirty days was 206 MB parsed at the top of every normal pass.
- **awrise**: `hostclock.last_tick` (read at the top of every firing for the gap) scans the newest ledger day backwards and parses only tick lines, not two days of rows.
- **awrise**: a job in the `normal` lane is written without a `lane` key. A store is read by every awrise on the host and an older one refuses an unknown key; saving `lane: normal` into all 104 records made the pass still running the previous version exit 2 at its end.

## [0.6.0] - 2026-11-16

### Added

- **awrise**: a job has a **lane**. `lane: critical` (`awrise add --lane critical`, `awrise set --name N lane=critical`) is for the wakes that page. The critical lane is its own pass under its own lock (`pass-critical.lock`) and dispatches ONLY critical jobs, so a normal pass in flight no longer refuses it and no slow normal wake can hold a pager back. A critical job must be `detach` (the pass spawns it and moves on) and must not use `wake`. `run-due --lane critical|normal|all` runs one lane or both; `all` is the default and, with no critical job, is exactly the old single pass.
- **awrise**: a host-clock firing no longer holds the clock. It runs the critical pass, hands the normal pass to a detached child (`run-due --lane normal`, the same spawn a `detach` wake gets) and exits within seconds, so Task Scheduler's single instance is free for the next minute's firing whatever the normal pass is doing. If the spawn fails the normal pass runs inline, as before; a normal pass already in flight is not started twice. The child reuses the firing's measured tick gap and writes only its own `tick_end`. A hand-run `run-due` still runs everything inline.
- Ledger `tick` and `tick_end` rows carry `lane` when a lane pass wrote them.

### Fixed

- Measured 2026-11-15/16 (853 passes in 2.5 days): passes p50 26 s, p90 6 min, p99 43 min, max 72 min, because every inline wake ran in one serial pass behind one Task Scheduler instance. The 1 min incident reflex, 2 min outage pager and 5 min condition pager started 4 to 45 minutes apart, and `missed` windows for unrelated jobs were counted from ticks that stopped for the length of the pass.

## [0.5.1] - 2026-11-15

### Fixed

- A detached job's receipt with `"deferred": true` (the job refused itself before doing
  any work, e.g. a full host memory budget) is a policy skip: the wake reads
  `skipped_held` / `deferred: <reason>`, the failure counter is left alone and no
  failure notice is sent. Before this it read "failure (unknown): receipt exit_code 2".
- A receipt's `skipped` text is now read as the wake's why.

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

