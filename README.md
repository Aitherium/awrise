# awrise

Wake something on a schedule, let it do one thing, and put it back to sleep.

A simple scheduler for one-off jobs. Store schedules in a JSON file (`~/.aither/awrise/jobs.json`). Runs jobs that are due using `run-due`. Idempotent: running it twice in the same window won't execute the same job twice.

## Installation

```bash
pip install -e .
```

## Usage

### Add a job

```bash
awrise add --name backup --every 1h --run "rsync -a src/ dest/"
awrise add --name check --every 15m --run "curl http://example.com/health"
awrise add --name deploy --every 2h --run "./deploy.sh"
```

Intervals: `15m` (minutes), `2h` (hours), `1d` (days).

### List jobs

```bash
awrise list
```

Output:
```
Name                 Interval     Command                              Last Run             Status
----                 --------     -------                              --------             ------
backup               1h           rsync -a src/ dest/                  never                pending
check                15m          curl http://example.com/health       never                pending
deploy               2h           ./deploy.sh                          never                pending
```

### Run due jobs

```bash
awrise run-due
```

This is idempotent. Run it from a cron job, a systemd timer, or anywhere. It executes all jobs whose `last_run` timestamp is more than `interval` ago. Records the execution time so running it twice in the same window won't run the same job twice.

Jobs that fail record the failure and do NOT block other jobs.

```bash
awrise run-due --quiet  # Suppress output
```

### Remove a job

```bash
awrise remove --name backup
```

## Configuration

Store schedules in `~/.aither/awrise/jobs.json` by default. Override with:

```bash
export AWRISE_HOME=/custom/path
```

## Self-test

```bash
awrise --self-test
```

Verifies:
- Interval parsing (valid and invalid formats)
- Idempotency (running twice in same window doesn't execute twice)
- Empty command rejection
- Job list/add/remove

## Design

The simplest useful scheduler: no daemon, no background process. Store state in JSON. The caller (cron, systemd, your own loop) decides when to call `run-due`.

- **Idempotent**: Last-run timestamp prevents duplicate executions
- **Resilient**: Failed jobs don't block other jobs
- **Honest**: Refuses unparseable intervals, empty commands, nonexistent jobs

