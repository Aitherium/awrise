# awrise

Wake something on a schedule, let it do one thing, and put it back to sleep.

A scheduled job fails as a **silence**. A wake that never fired because the host was off, a
wake that overlapped the run before it, and a wake that hung forever holding its slot are
indistinguishable from outside: in every case nothing happened and nothing said so. Cron has
no memory -- a missed minute is simply gone -- so "not due yet" and "due, and never ran" read
identically.

awrise's product is not execution, it is **the record**. The host scheduler (cron, a systemd
timer, launchd, Task Scheduler) is the clock; `awrise run-due` is one pass that exits; the
wake ledger is the memory. Every wake ends in exactly one terminal row carrying a reason, and
`awrise history` answers what happened to all of them.

No daemon. No background process. Standard library only -- no runtime dependencies.

## Install

```bash
pip install awrise
```

Python 3.10 or newer. Linux, macOS and Windows.

## Quick start

```bash
awrise add --name backup --every 1h --run "rsync -a src/ dest/"
awrise install --cron       # or --systemd-user / --systemd-system / --launchd / --schtasks
awrise install --check      # 0 installed and ticking, 1 a measured no, 2 unjudged
awrise history              # what happened to every wake and why
awrise status               # per-job verdict; exit 1 on a failing job
```

`install` writes nothing until you ask it to: `--print` renders the entry to stdout,
`--dry-run` names every file and command it would touch, and an install is reported as
successful only after it has been **read back** from the scheduler.

## Commands

| command | what it does |
|---|---|
| `add --name N --every 15m --run "..."` | register a job; refuses an empty command, a duplicate name, an unparseable interval, and a timeout at or above the interval (`--allow-overrun` to insist) |
| `set --name N key=value ...` | change one spec key; an unknown key is refused, never stored and ignored |
| `enable --name N` / `disable --name N` | a disabled job that comes due writes `skipped_disabled`, never silence |
| `remove --name N` | forget the job (refused while a wake of it is in flight) |
| `list [--json]` | the jobs, with the interval exactly as entered, the next due time, and the windows missed |
| `run-due [--quiet] [--dry-run] [--prune 30d]` | one pass: reconcile, tick, run every due job, exit |
| `run --name N [--force]` | run one job now, due or not |
| `history [--job N] [--since 7d] [--event E] [--json] [--judge]` | the ledger; `--judge` turns it into a verdict (drift or absence) and exits 2 on a window too young to judge |
| `status` | per-job verdict and the host-clock line; exit 1 on a failing job |
| `explain --name N` | why that job did or did not fire on the last tick |
| `checks [--since 7d] [--json]` | WL001-WL004 against the record: an unclosed wake, a reason outside the closed set, a job that should have woken and did not, a stale lock |
| `prewarm [--apply] [--json] [--ledger-dir D] [--exclude GLOB] [--park-after] [--allow-derived-units]` | read the usage ledger and propose one daily wake per unit something asked for yesterday; prints, and schedules nothing without `--apply` |
| `prune [--keep 30d] [--dry-run] [--force]` | drop ledger day files older than a window |
| `install [--cron\|--systemd-user\|--systemd-system\|--launchd\|--schtasks] [--every 1m] [--print] [--dry-run] [--check] [--uninstall]` | register (or judge, or remove) the host clock that runs `run-due` |
| `reconcile [--restore] [--reset]` | close orphaned wakes and break dead locks; `--restore` brings back the backup store |
| `doctor` | what of the aw* family is installed, and the one thing to fix |

Every command exits **0 clean, 1 a measured no, 2 could not judge** -- never 0 on silence.

## The job spec

`add` and `set` accept exactly these keys, and refuse every other one:

| key | default | meaning |
|---|---|---|
| `every` | required | `Ns` / `Nm` / `Nh` / `Nd` / `Nw` and compounds (`1h30m`); stored as a number, displayed exactly as entered |
| `run` | required | the command; empty is refused at `add` time, not skipped at run time |
| `timeout_s` | `300` | the whole process tree is killed at the bound, on POSIX and on Windows |
| `cwd` | none | the only environment knob; a missing directory is an `error` row, not a crash |
| `at` | none | `HH:MM` UTC daily anchor: due is the next anchor after the last start, so a laptop asleep at 07:00 catches up once and re-anchors instead of walking later every day |
| `missed` | `catch_up_once` | windows lost while nothing ran: catch up exactly once, or `skip` them |
| `detach` | `false` | spawn and close the wake at once; the child outlives the pass |
| `enabled` | `true` | a disabled job still leaves a row when it comes due |
| `executor` | `shell` | `python`, `http`, `awrun`, `agent` and `session` are optional, and import nothing until a job names them |
| `wake` | none | a unit to wake before the job runs and wait for; the wake is measured and recorded (`--wake my-model.service`) |
| `park_after` | `false` | with `wake`: ask for the unit to be put back to sleep afterwards; refused without `wake`, refused with `detach`, and skipped whenever the wake closed with the work still outstanding (a queued run-queue item) |
| `wake_required` | `true` | a failed wake means the command does NOT run; `false` runs it anyway and still records the failure |
| `report` | off | `--report-relay '#channel'` and `--card-after N`: a relay line, and one decision card after a failure streak |

Overlap is not one of them: it is fixed behaviour. A job whose previous run still holds its lock is recorded
`skipped_overlap` and is never started a second time.

## Waking what the job needs

A job that talks to something which is asleep can name it:

```bash
awrise add --name nightly-render --every 1d --at 03:00 \
           --wake my-model.service --park-after \
           --run "render --all"
awrise prewarm                     # what yesterday's usage says should be pre-warmed
awrise prewarm --apply             # schedule exactly that
```

The unit is started by a **host agent** that owns the service manager; awrise never does. They
meet on a directory both can write -- one file per request, one per answer -- so there is no
port, no token and no daemon of awrise's own:

```
<plane>/requests/<id>.json   {id, action, unit, wait_s, created_at}
<plane>/results/<id>.json    {id, ok, state, reason, wake_s}
<plane>/agent.json           {alive_at}
```

Three rules make the failures loud rather than slow:

* **no fresh heartbeat, no request.** Nothing is written where nothing reads it, so a machine
  with no agent fails the wake in milliseconds instead of spending its whole budget.
* **an unanswered request is `failed`, never `done`, and it is withdrawn** -- an agent that
  wakes up late cannot start a unit for a job that already gave up on it.
* **a failed wake does not run the command** (unless `wake_required: false`). A command run
  against a unit that is not up fails in a way that reads like the command's own bug.

The wake time is the agent's own measurement when it made one, and it lands in the finished
row as `wake_s` beside `wake_unit`, `wake_state` and `park_state` -- so "the command took 4s"
and "waking what it talks to took eleven minutes" are two numbers in the record instead of one
duration nobody can explain.

`prewarm` reads the **usage ledger** -- one `<service>.json` per service, written by the
services themselves, holding when each was last asked for something. For every service used on
the chosen day it proposes one daily job at that hour, and prints the EVIDENCE it used
(`basis=first_request_at` when the record knows its first use of the day, `last_request_at`
when that is all it has). A service with no entry for that day gets no proposal: silence is not
a schedule. Nothing is written without `--apply`, and an absent ledger directory is exit 2 --
could not judge -- never an empty plan.

Every proposal also says where its UNIT NAME came from. `unit=declared` means the ledger
record named the unit; `unit=derived` means awrise assembled one from the service name, which
is a guess -- service names and unit names are written by different hands and they do not
always agree. `--apply` schedules the declared ones and REFUSES the derived ones (exit 1, with
the list), because a nightly job that names a unit nobody runs fails into a log and reads
exactly like a quiet machine. `--allow-derived-units` says you checked them yourself.

## The ledger

`$AWRISE_HOME/ledger/YYYY-MM-DD.jsonl`, one JSON object per line, flushed and fsynced per
row. The writer refuses an event or a state outside the closed set, and refuses an empty
reason, so a row can only come from something that was measured. Output tails are redacted
before they are written.

```bash
awrise history --since 7d --json
```

Each pass also writes a `tick` and a `tick_end` row carrying the gap since the previous one,
so a clock that stopped is a visible absence rather than a silence.

## Configuration

```bash
export AWRISE_HOME=/custom/path      # default ~/.aither/awrise, created 0700

# only for `wake` / `park_after` / `prewarm` -- unset on a machine that has no unit agent
export AITHER_GPU_UNIT_PLANE_DIR=/srv/plane      # where the unit agent answers
export AITHER_LIBRARY=/srv/library               # ...or <AITHER_LIBRARY>/Data/compute/gpu_unit_plane
export AITHER_USAGE_LEDGER_DIR=/srv/usage        # default: the plane's sibling `usage/`
export AWRISE_WAKE_BUDGET_S=600                  # how long a unit may take to come up healthy
```

With none of these set there is no plane: a job that names `wake` is an error row saying so,
which is the point -- the alternative is a request written into a guessed directory that
nobody ever reads.

awrise refuses to run when the home or the store is writable by anyone but its owner. Keep it
on a native path: one home shared by a Windows and a WSL view of the same directory is
protected by neither one's permissions.

## Self-test

```bash
awrise --self-test           # run every case: 0 all passed, 1 a case failed, 2 a case broke
awrise --self-test --list    # the case names, one per line
```

Every case has a negative twin, so a case that can no longer fail is itself a failure. The
list below is the output of `awrise --self-test --list` and must stay equal to it (AWR001).

### Self-test verifies

- `interval_parse_accepts_s_m_h_d_w_and_compounds`
- `interval_parse_rejects_empty_zero_negative_garbage`
- `store_crash_mid_save_keeps_jobs`
- `store_corrupt_exits_2_and_restore_returns_bak`
- `store_v1_migrates_with_tz_aware_stamps_and_keeps_v1_bak`
- `ledger_refuses_unknown_event_state_and_empty_reason`
- `ledger_accepts_the_closed_vocabulary`
- `ledger_redacts_output_tails`
- `run_due_failure_exit_code_propagates`
- `every_terminal_state_recorded_exactly_once`
- `reconcile_closes_orphan_and_does_not_refire`
- `spec_keys_accepted_equal_keys_read`
- `add_refuses_empty_command_duplicate_and_bad_timeout`
- `run_due_is_idempotent_inside_the_window`
- `python_m_awrise_entry_exists`
- `started_row_precedes_exec_and_stamp_follows_finished`
- `finished_wake_whose_stamp_was_lost_is_not_refired`
- `ledger_concurrent_appends_lose_nothing`
- `status_is_unjudged_for_a_job_that_never_woke`
- `store_refuses_malformed_records_with_exit_2`
- `add_refuses_names_outside_the_grammar`
- `reconcile_never_copies_an_unreadable_ts_into_the_store`
- `add_refuses_timeout_ge_interval`
- `overlap_is_skipped_while_the_lock_is_held_and_a_stale_lock_is_broken`
- `timeout_kills_the_whole_process_tree`
- `detach_closes_at_spawn_and_leaves_the_child_alive`
- `clock_skew_is_an_error_row_then_a_fire`
- `healthy_store_is_byte_stable_across_passes`
- `readded_job_never_inherits_a_removed_jobs_wake`
- `a_stale_lock_is_broken_by_exactly_one_pass`
- `release_never_removes_another_wakes_lock`
- `a_foreign_ledger_row_does_not_wedge_the_next_pass`
- `clock_skew_on_a_disabled_job_heals_in_one_pass`
- `timeout_past_the_platform_wait_is_refused`
- `hostclock_renders_every_adapter_the_same_way_twice`
- `schtasks_payload_is_crlf_hidden_and_inside_the_length_cap`
- `install_print_and_dry_run_touch_nothing`
- `install_check_is_unjudged_without_a_record_and_red_without_the_payload`
- `install_refuses_to_claim_an_entry_it_cannot_read_back`
- `every_pass_records_a_tick_with_the_gap_since_the_last_one`
- `explain_reports_overdue_windows`
- `missed_windows_are_one_row_and_one_catch_up_fire`
- `missed_policy_skip_drops_the_windows_without_running`
- `at_anchor_fires_once_past_the_anchor_and_reanchors`
- `dry_run_says_what_would_fire_and_moves_nothing`
- `a_missed_row_needs_a_measured_clock_gap`
- `a_wake_in_flight_is_not_recorded_as_lost_windows`
- `dry_run_agrees_with_the_pass_on_a_skewed_disabled_job`
- `one_unreadable_line_cannot_buy_a_verdict`
- `the_window_guard_is_per_job_not_the_longest_cadence`
- `prune_is_unjudged_when_the_clock_cannot_be_read`
- `history_judge_flags_drift`
- `history_judge_is_unjudged_on_a_young_ledger`
- `checks_rules_each_fire_and_each_stay_quiet`
- `prune_removes_only_files_outside_the_window`
- `list_json_carries_the_exact_interval_and_the_next_due`
- `an_unreadable_clock_override_is_a_verdict_not_a_traceback`
- `optional_executors_never_import_their_packages_at_module_level`
- `an_unknown_executor_never_falls_through_to_the_shell`
- `a_bearer_path_outside_the_confinement_is_refused`
- `a_permission_mode_outside_the_allowlist_is_refused_at_add`
- `a_sink_that_cannot_run_is_a_row_not_a_lost_pass`
- `a_job_with_no_sinks_configured_writes_no_report_rows`
- `http_gaierror_is_skipped_unresolvable`
- `http_5xx_is_failure_with_body_tail`
- `card_raised_after_n_failures`
- `report_block_validated_at_add`
- `cwd_missing_is_error`

## Design

- **The record is the product.** Nothing is templated: every row comes from a measured
  outcome, and a pass that cannot judge something says so rather than reporting success.
- **No daemon.** A daemon that dies is the failure it was meant to prevent. A host clock
  that stops is a gap between ticks, and the ledger can see it.
- **One pass, then exit.** `run-due` reconciles, fires what is due, and returns an exit code
  a cron wrapper can act on.
- **Nothing optional is imported until it is used.** The `http`, `awrun`, `agent` and
  `session` executors and the relay and card sinks are guarded, off by default, and carry no
  default URL.
- **Atomic state.** `jobs.json` is written through a temporary file, fsynced and replaced,
  after every outcome; the previous copy is kept as `jobs.json.bak`. A corrupt store exits 2
  and is restorable -- it is never silently replaced by an empty one.

## Licence

MIT. See `LICENSE`.
