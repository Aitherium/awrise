# awrise

<!-- aither-header:start GENERATED from the ecosystem registry. Edits here are overwritten; change the registry instead. -->

**[Docs](https://aitherium.github.io/awrise/)**  ·  [Source](https://github.com/Aitherium/awrise)  ·  `pip install awrise`  ·  [The Aither World](https://aitherium.github.io/)

> **The Aither World** is an operating system for agents — a Linux you can hand to one, the runtimes it works in, and the tools it works with. [awnix](https://github.com/Aitherium/awnix) is the Linux underneath it; **awrise** is one of its 65 bricks — each installs on its own, runs offline, and needs no account.
>
> **Start here:** Wake something on a schedule, let it do one thing, and put it back to sleep.

<!-- aither-header:end -->

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
awrise install-clock        # register THIS machine's scheduler, idempotently
awrise install --cron       # or --systemd-user / --systemd-system / --launchd / --schtasks
awrise install --check      # 0 installed and ticking, 1 a measured no, 2 unjudged
awrise history              # what happened to every wake and why
awrise status               # per-job verdict; exit 1 on a failing job
```

`install` writes nothing until you ask it to: `--print` renders the entry to stdout,
`--dry-run` names every file and command it would touch, and an install is reported as
successful only after it has been **read back** from the scheduler.

`install-clock` is `install` for an operator who does not want to know which scheduler
the host has: it picks the native one, refuses to run twice (a second call that finds
the same entry, the same interval, the same interpreter and the same payload bytes says
so and changes nothing), and when the scheduler refuses the create it prints the exact
command to run from an elevated shell rather than a diagnosis.

On Windows the two entries it registers are not equal: the repeating one registers for
an ordinary user, and the at-startup one needs elevation. A refused at-startup entry
therefore does NOT cost the host its clock -- the repeating entry is installed, and the
gap (no wake after an unattended reboot, until somebody logs on) is printed by
`install-clock`, `install --check` and `doctor` on every run, with its one-line fix.

**A pass you ran by hand is not the clock ticking.** Every adapter launches
`run-due --invoker <kind>`, and freshness is judged on those rows only: a ledger full of
hand-run passes reads as a clock that has never fired, which is what it is.

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
| `install-clock [--every 5m] [--force]` | register THIS machine's host clock, idempotently; on a refusal it prints the exact command to run elevated |
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
| `at` | none | `HH:MM` UTC daily anchor: due is the next anchor after the last start — or, until it has ever run, after it was added — so a job added at 14:00 waits for 03:00 and a laptop asleep at 07:00 catches up once and re-anchors instead of walking later every day |
| `missed` | `catch_up_once` | windows lost while nothing ran: catch up exactly once, or `skip` them |
| `detach` | `false` | spawn and close the wake at once; the child outlives the pass, and keeps the job's lock until it exits — so the overlap rule below holds for it too |
| `enabled` | `true` | a disabled job still leaves a row when it comes due |
| `executor` | `shell` | `python`, `http`, `awrun`, `agent` and `session` are optional, and import nothing until a job names them |
| `wake` | none | a unit to wake before the job runs and wait for; the wake is measured and recorded (`--wake my-model.service`) |
| `park_after` | `false` | with `wake`: ask for the unit to be put back to sleep afterwards; refused without `wake`, refused with `detach`, and skipped whenever the wake closed with the work still outstanding (a queued run-queue item) |
| `wake_required` | `true` | a failed wake means the command does NOT run; `false` runs it anyway and still records the failure |
| `report` | off | `--report-relay '#channel'` and `--card-after N`: a relay line, and one decision card after a failure streak |
| `report.memory` | `false` (`set NAME report.memory=true`) | recall this job's own past wakes into its env before it runs, and remember the one that just finished -- see [Memory](#memory) |
| `predict` | `off` (`set NAME predict=warn\|skip`) | consult this job's own history before it fires -- see [Predict](#predict) |

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

## Memory

`report.memory: true` gives a job a small, ancestor-safe memory of its own past wakes, backed
by [awm](https://github.com/Aitherium/awm) -- off by default, and inert until set:

```bash
awrise set nightly-render report.memory=true
```

Before the job runs, awrise recalls its own last 20 finished wakes and exports them as JSON in
`AWRISE_MEMORY_JSON` (an empty array `[]` on the first run, never absent). After it finishes, the
outcome (`state`, `reason`, `duration_s`, `exit_code`, `ts`) is remembered under a key unique to
that wake, so the next wake's recall includes it. `awrise explain --name NAME` shows the same
payload live, read-only, for a job with `report.memory: true`.

The memory is scoped `awrise:<host>:<job>` -- one job, one host -- and the store lives at
`$AWRISE_HOME/awm/memory.db`, isolated per `AWRISE_HOME`. `AWRISE_MEMORY_JSON` is **inert data
only**: awrise never evaluates, sources or shell-interpolates it, and a `shell_command` job that
does (`eval $AWRISE_MEMORY_JSON`) is that job's own choice and risk, not something this sink does
or endorses.

Both the recall and the remember are bounded by a tight timeout and fail OPEN: a missing or
broken `awm`, or a store that cannot answer in time, never blocks or fails the wake -- it costs
one `report_error` ledger row and nothing else.

## Predict

`predict: warn` or `predict: skip` gives a job a gate over its OWN history, backed by
[awpredict](https://github.com/Aitherium/awpredict) -- off by default, and inert until set:

```bash
awrise set nightly-render predict=warn    # log the verdict, always fire
awrise set nightly-render predict=skip    # refuse to fire on a bad-outcome verdict
awrise predict --name nightly-render      # the live verdict, read-only, right now
```

Before the job runs, awrise counts its own judged finished wakes (a `success`, or one of the
`failure` / `timeout` / `error` / `orphaned` bad states -- a policy skip or a cancellation says
nothing about the command's own behaviour, so it is not counted). Below five of them the verdict
is `UNJUDGED, fewer than 5 historical rows` and the job fires exactly as if `predict` were `off`
-- there is no awpredict sentinel for "not enough data yet"; awrise decides that itself, before
ever calling the engine. At or above five, awrise asks a cached `awpredict` `MLPWorldModel` what
its own history says the next wake's outcome looks like, and reads `good` or `bad` off the
predicted reward.

`predict: warn` only LOGS the verdict -- on the job's `started` ledger row, as `prediction:
{verdict, reason, confidence, mode, rows}` -- and always fires anyway; a bad-outcome verdict is
information, not a veto. `predict: skip` is the only policy that can refuse to fire: on a
bad-outcome verdict it writes a `skipped_predicted` row instead of running the job -- UNLESS the
job's own PREVIOUS row was already `skipped_predicted`, in which case a real attempt is forced
regardless of what the new prediction says, so a job can never be skipped twice running on a
prediction alone. `awrise --dry-run` agrees with the real pass: a job that would be
`skipped_predicted` next pass shows as `hold`, never `would_fire`.

The call is bounded by a tight timeout and fails OPEN: a missing or broken `awpredict`, a cold
engine with nothing to say yet, or a call that does not return in time all degrade to
`UNJUDGED` -- the job fires exactly as if `predict` were `off`. A genuine failure (a timeout or
an exception, never "too few rows") also costs one `report_error` ledger row, same as the
memory sink above -- never a failed wake.

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
- `doctor_never_exits_zero_while_printing_a_measured_no`
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
- `memory_off_by_default_touches_neither_env_nor_store`
- `memory_on_exports_recalled_wakes_and_excludes_ancestor_scope`
- `memory_failure_is_a_report_error_row_not_a_failed_wake`
- `explain_shows_the_same_recalled_memory_payload`
- `predict_off_writes_no_prediction_and_builds_no_engine`
- `predict_under_threshold_is_unjudged_and_still_fires`
- `predict_engine_is_built_exactly_once_across_due_checks`
- `predict_timeout_fails_open_and_writes_a_report_error_row`
- `predict_skip_holds_on_a_bad_verdict_and_never_skips_twice_running`
- `dry_run_predict_skip_agrees_with_the_real_pass`

## Design

- **The record is the product.** Nothing is templated: every row comes from a measured
  outcome, and a pass that cannot judge something says so rather than reporting success.
- **No daemon.** A daemon that dies is the failure it was meant to prevent. A host clock
  that stops is a gap between ticks, and the ledger can see it.
- **One pass, then exit.** `run-due` reconciles, fires what is due, and returns an exit code
  a cron wrapper can act on.
- **Nothing optional is imported until it is used.** The `http`, `awrun`, `agent` and
  `session` executors, the relay and card sinks, and the `awm`-backed memory sink are guarded,
  off by default, and carry no default URL.
- **Atomic state.** `jobs.json` is written through a temporary file, fsynced and replaced,
  after every outcome; the previous copy is kept as `jobs.json.bak`. A corrupt store exits 2
  and is restorable -- it is never silently replaced by an empty one.

## Licence

MIT. See `LICENSE`.

## The aw family

Standalone tools that share one idea: **replace something you would otherwise have to _trust_ with something you can _check_.**

Each installs on its own, works offline, and needs no account.

| | instead of trusting | you check |
|---|---|---|
| [awdk](https://github.com/Aitherium/awdk) | a framework's idea of how your agents should run | one loop you can read, pointed at a backend you already pay for |
| [awskills](https://github.com/Aitherium/awskills) | that an agent knows your procedure | the procedure written down, versioned, and loadable by any agent |
| [awpack](https://github.com/Aitherium/awpack) | that the pack you want shipped inside somebody's SDK, under whatever licence that SDK happens to carry | the pack as its own versioned artifact, with its own licence, that any agent runtime can install |
| [awm](https://github.com/Aitherium/awm) | that memory stayed in its lane | tenant:user:project scopes, so a write cannot cross a boundary |
| [awdesk](https://github.com/Aitherium/awdesk) | that the agent is somewhere behind a browser tab | a tray icon, a face on your desktop, and the decision card that pops when it needs you |
| [awnode](https://github.com/Aitherium/awnode) | a vendor's cloud with every prompt | a local gateway routing to backends you chose |
| [awgraph](https://github.com/Aitherium/awgraph) | that grep found everything | an AST + tree-sitter call graph an agent can traverse |
| [awgit](https://github.com/Aitherium/awgit) | that no one else is editing this file | a lease, refused at commit time if you do not hold it |
| [awdelphi](https://github.com/Aitherium/awdelphi) | one agent's confident take on a decision | the round trace, the anonymity, and who dissents |
| [awtoll](https://github.com/Aitherium/awtoll) | that your tooling is saving you context | the measured token cost of each tool call, and what the alternative cost |
| [awseal](https://github.com/Aitherium/awseal) | that the artifact came from who you think | an Ed25519 seal — the key that verifies is not the key that forges |
| [awshare](https://github.com/Aitherium/awshare) | that the download is intact | content-addressed bundles, verified on fetch |
| [awnest](https://github.com/Aitherium/awnest) | that there is a person on the other end | a verdict with evidence, where "we could not tell" is not "yes" |
| [awrena](https://github.com/Aitherium/awrena) | a leaderboard someone can edit, and votes nobody counted | a scored duel with both answers kept, and a result bound to them |
| [awnboard](https://github.com/Aitherium/awnboard) | a share link anyone who sees it can use | an invitation addressed to one person, for one gate, revocable |
| [awnix](https://github.com/Aitherium/awnix) | that the box is what you left it as | an immutable image you built, with atomic rollback |
| [awrecover](https://github.com/Aitherium/awrecover) | that the restore worked | a restore that fully lands or does not land at all |
| [awstorage](https://github.com/Aitherium/awstorage) | a du you ran last month, and a peers file that says 3 TB free | an inventory snapshot per node with a diff since the last one, and each tree classified re-fetchable or not |
| [awrelay](https://github.com/Aitherium/awrelay) | a SaaS in the middle of your agents | findings, alerts and coordination over your own transport |
| [awask](https://github.com/Aitherium/awask) | that anyone read the paragraph where you asked | the ask itself, with a button that steers the session that raised it |
| [awmail](https://github.com/Aitherium/awmail) | a mailbox somebody else can read | mail your agents send and receive over your own server |
| [awfind](https://github.com/Aitherium/awfind) | one vendor's idea of the web | results from whichever providers you configured |
| [awbrowse](https://github.com/Aitherium/awbrowse) | that the page said what you were told | the render, the DOM and the requests it made |
| [awvoice](https://github.com/Aitherium/awvoice) | that a cloud vendor may hold your audio | a transcript and a wav from a service you host |
| [awvision](https://github.com/Aitherium/awvision) | a filename and a caption somebody wrote | what a model actually reports about the pixels |
| [awscreen](https://github.com/Aitherium/awscreen) | a selector that was true when the page was written | the elements actually rendered, by what they look like |
| [gawbbonet](https://github.com/Aitherium/gawbbonet) | the model to keep a 300-message campaign coherent by itself | campaign facts recalled from scoped memory you can list and edit |
| [aitherkvcache](https://github.com/Aitherium/aitherkvcache) | a vendor's quantisation defaults | sub-byte KV cache kernels you can benchmark yourself |
| [awrtifact](https://github.com/Aitherium/awrtifact) | a hand-rolled split script and a hand-edited worker manifest | byte-verified parts in a release, served with Range + CORS, sizes asserted by a live gate |
| [AitherZero](https://github.com/Aitherium/AitherZero) | a pile of scripts nobody has numbered | numbered, discoverable automation with declarative playbooks |
| [AitherConnect](https://github.com/Aitherium/AitherConnect) | what a page tells your browser to do | a federated search and desktop bridge you host |
| [awreason](https://github.com/Aitherium/awreason) | a confident paragraph | the phases it went through, and every tool call it made to get there |
| [awrecurse](https://github.com/Aitherium/awrecurse) | that everything you pasted in was actually read | which slices it opened, and what it concluded from each |
| [awprism](https://github.com/Aitherium/awprism) | the first explanation that fits | the ranked alternatives, and the observation that separates them |
| [awrepl](https://github.com/Aitherium/awrepl) | what the agent believes the value is | the value, printed from the live session |
| [awresearch](https://github.com/Aitherium/awresearch) | a summary of pages nobody opened | every claim against the source it came from |
| [awfocus](https://github.com/Aitherium/awfocus) | twelve terminal tabs and a bad memory | one command that names every session, finds any transcript, and opens or steers the one you want |
| [awgym](https://github.com/Aitherium/awgym) | that a world model learned anything from the games it saw | transitions captured from real play, fed back, and the retrodiction score falling on grids it never saw |
| [awpredict](https://github.com/Aitherium/awpredict) | a model because it trained without erroring | its prediction against a self-updating lookup, on the rows that are actually novel |
| [awsh](https://github.com/Aitherium/awsh) | that you already know the name of the command | what it decided your line meant, before it acts on it |
| **awrise** _(you are here)_ | that a scheduled agent ran at all, and ran exactly once | a durable record of every wake -- fired, skipped, overlapped or timed out -- each with its reason |
| [awkno](https://github.com/Aitherium/awkno) | that the docs site is up, or that you remember the family | the whole ecosystem in your terminal, with no network at all |
| [awwall](https://github.com/Aitherium/awwall) | that a service only talks to the hosts you think it talks to | an explicit egress allowlist, where a denial names the rule that denied it |
| [awembed](https://github.com/Aitherium/awembed) | a general-purpose embedder that has never seen your code | a held-out split of whole directories, scored teacher vs student vs int8 |
| [awtax](https://github.com/Aitherium/awtax) | a closed tax app's sealed file you can never read again | a plain, provider-neutral schema of every figure, with the page it came from |
| [awsettings](https://github.com/Aitherium/awsettings) | that you will remember to re-approve the same thing on every box you work from | one profile, unioned rather than overwritten, with the credentials left behind |
| [awavatar](https://github.com/Aitherium/awavatar) | a cloud 3D vendor's opaque task id | a manifest with a sha256, a licence and a rig-audit verdict per file |

[**awnix**](https://github.com/Aitherium/awnix) is the ground floor — A Linux you can hand to an agent — immutable base, capabilities included.

<!-- aither-ecosystem:start GENERATED from the ecosystem registry. Edits here are overwritten; change the registry instead. -->

## The aw family

Standalone tools that share one idea: **replace something you would otherwise have to _trust_ with something you can _check_.**

Each installs on its own, works offline, and needs no account.

| | instead of trusting | you check |
|---|---|---|
| [awdk](https://github.com/Aitherium/awdk) | a framework's idea of how your agents should run | one loop you can read, pointed at a backend you already pay for |
| [awskills](https://github.com/Aitherium/awskills) | that an agent knows your procedure | the procedure written down, versioned, and loadable by any agent |
| [awpack](https://github.com/Aitherium/awpack) | that the pack you want shipped inside somebody's SDK, under whatever licence that SDK happens to carry | the pack as its own versioned artifact, with its own licence, that any agent runtime can install |
| [awm](https://github.com/Aitherium/awm) | that memory stayed in its lane | tenant:user:project scopes, so a write cannot cross a boundary |
| [awdesk](https://github.com/Aitherium/awdesk) | that the agent is somewhere behind a browser tab | a tray icon, a face on your desktop, and the decision card that pops when it needs you |
| [awnode](https://github.com/Aitherium/awnode) | a vendor's cloud with every prompt | a local gateway routing to backends you chose |
| [awgraph](https://github.com/Aitherium/awgraph) | that grep found everything | an AST + tree-sitter call graph an agent can traverse |
| [awgit](https://github.com/Aitherium/awgit) | that no one else is editing this file | a lease, refused at commit time if you do not hold it |
| [awdelphi](https://github.com/Aitherium/awdelphi) | one agent's confident take on a decision | the round trace, the anonymity, and who dissents |
| [awclassify](https://github.com/Aitherium/awclassify) | a filename, a folder, or whoever last touched it | doc_type, visibility, audience and topics, with the evidence lines that decided each |
| [awtoll](https://github.com/Aitherium/awtoll) | that your tooling is saving you context | the measured token cost of each tool call, and what the alternative cost |
| [awseal](https://github.com/Aitherium/awseal) | that the artifact came from who you think | an Ed25519 seal — the key that verifies is not the key that forges |
| [awshare](https://github.com/Aitherium/awshare) | that the download is intact | content-addressed bundles, verified on fetch |
| [awnest](https://github.com/Aitherium/awnest) | that there is a person on the other end | a verdict with evidence, where "we could not tell" is not "yes" |
| [awrena](https://github.com/Aitherium/awrena) | a leaderboard someone can edit, and votes nobody counted | a scored duel with both answers kept, and a result bound to them |
| [awnboard](https://github.com/Aitherium/awnboard) | a share link anyone who sees it can use | an invitation addressed to one person, for one gate, revocable |
| [awnix](https://github.com/Aitherium/awnix) | that the box is what you left it as | an immutable image you built, with atomic rollback |
| [awrecover](https://github.com/Aitherium/awrecover) | that the restore worked | a restore that fully lands or does not land at all |
| [awstorage](https://github.com/Aitherium/awstorage) | a du you ran last month, and a peers file that says 3 TB free | an inventory snapshot per node with a diff since the last one, and each tree classified re-fetchable or not |
| [awrelay](https://github.com/Aitherium/awrelay) | a SaaS in the middle of your agents | findings, alerts and coordination over your own transport |
| [awask](https://github.com/Aitherium/awask) | that anyone read the paragraph where you asked | the ask itself, with a button that steers the session that raised it |
| [awmail](https://github.com/Aitherium/awmail) | a mailbox somebody else can read | mail your agents send and receive over your own server |
| [awswarm](https://github.com/Aitherium/awswarm) | that a model either fits your GPU or it doesn't run at all | a placement plan and an acquisition-probability estimate before you spend on a run |
| [awfind](https://github.com/Aitherium/awfind) | one vendor's idea of the web | results from whichever providers you configured |
| [awbrowse](https://github.com/Aitherium/awbrowse) | that the page said what you were told | the render, the DOM and the requests it made |
| [awvoice](https://github.com/Aitherium/awvoice) | that a cloud vendor may hold your audio | a transcript and a wav from a service you host |
| [awvision](https://github.com/Aitherium/awvision) | a filename and a caption somebody wrote | what a model actually reports about the pixels |
| [awscreen](https://github.com/Aitherium/awscreen) | a selector that was true when the page was written | the elements actually rendered, by what they look like |
| [awbeads](https://github.com/Aitherium/awbeads) | that a layout your users built survives the next deploy | the arrangement as data you can read back, diff, and hand to another surface |
| [awbonsai](https://github.com/Aitherium/awbonsai) | that inference always means a request left the machine | a WebGPU model answering on the tab's own GPU, with a consent record logged before it ever loaded |
| [gawbbonet](https://github.com/Aitherium/gawbbonet) | the model to keep a 300-message campaign coherent by itself | campaign facts recalled from scoped memory you can list and edit |
| [aitherkvcache](https://github.com/Aitherium/aitherkvcache) | a vendor's quantisation defaults | sub-byte KV cache kernels you can benchmark yourself |
| [awrtifact](https://github.com/Aitherium/awrtifact) | a hand-rolled split script and a hand-edited worker manifest | byte-verified parts in a release, served with Range + CORS, sizes asserted by a live gate |
| [AitherZero](https://github.com/Aitherium/AitherZero) | a pile of scripts nobody has numbered | numbered, discoverable automation with declarative playbooks |
| [AitherConnect](https://github.com/Aitherium/AitherConnect) | what a page tells your browser to do | a federated search and desktop bridge you host |
| [awreason](https://github.com/Aitherium/awreason) | a confident paragraph | the phases it went through, and every tool call it made to get there |
| [awrecurse](https://github.com/Aitherium/awrecurse) | that everything you pasted in was actually read | which slices it opened, and what it concluded from each |
| [awprism](https://github.com/Aitherium/awprism) | the first explanation that fits | the ranked alternatives, and the observation that separates them |
| [awrepl](https://github.com/Aitherium/awrepl) | what the agent believes the value is | the value, printed from the live session |
| [awreport](https://github.com/Aitherium/awreport) | that the report you pasted carried no token in it | a redacted report, and the duplicate it merged into instead of filing twice |
| [awresearch](https://github.com/Aitherium/awresearch) | a summary of pages nobody opened | every claim against the source it came from |
| [awfocus](https://github.com/Aitherium/awfocus) | twelve terminal tabs and a bad memory | one command that names every session, finds any transcript, and opens or steers the one you want |
| [awgym](https://github.com/Aitherium/awgym) | that a world model learned anything from the games it saw | transitions captured from real play, fed back, and the retrodiction score falling on grids it never saw |
| [awpredict](https://github.com/Aitherium/awpredict) | a model because it trained without erroring | its prediction against a self-updating lookup, on the rows that are actually novel |
| [awevolve](https://github.com/Aitherium/awevolve) | that your optimisation loop is finding anything | every version it kept, the score that version earned, and the edit that produced it |
| [awsh](https://github.com/Aitherium/awsh) | that you already know the name of the command | what it decided your line meant, before it acts on it |
| [awmine](https://github.com/Aitherium/awmine) | that a session's lesson survived the session | a row per outcome, a candidate per lesson, and the transcript line each one came from |
| **awrise** _(you are here)_ | that a scheduled agent ran at all, and ran exactly once | a durable record of every wake -- fired, skipped, overlapped or timed out -- each with its reason |
| [awkno](https://github.com/Aitherium/awkno) | that the docs site is up, or that you remember the family | the whole ecosystem in your terminal, with no network at all |
| [awwall](https://github.com/Aitherium/awwall) | that a service only talks to the hosts you think it talks to | an explicit egress allowlist, where a denial names the rule that denied it |
| [awembed](https://github.com/Aitherium/awembed) | a general-purpose embedder that has never seen your code | a held-out split of whole directories, scored teacher vs student vs int8 |
| [awtax](https://github.com/Aitherium/awtax) | a closed tax app's sealed file you can never read again | a plain, provider-neutral schema of every figure, with the page it came from |
| [awsettings](https://github.com/Aitherium/awsettings) | that you will remember to re-approve the same thing on every box you work from | one profile, unioned rather than overwritten, with the credentials left behind |
| [awavatar](https://github.com/Aitherium/awavatar) | a cloud 3D vendor's opaque task id | a manifest with a sha256, a licence and a rig-audit verdict per file |

[**awnix**](https://github.com/Aitherium/awnix) is the ground floor — A Linux you can hand to an agent — immutable base, capabilities included.

## The Aitherium ecosystem

Every repository here is public. Each publishes an `aither-manifest.json` beside its page, so any surface can read every sibling's — the network is browsable from any node in it.

| repo | what it is | pages |
|---|---|---|
| [awdk](https://github.com/Aitherium/awdk) | Build AI agent fleets — 3 lines, any backend, local or cloud | [docs](https://aitherium.github.io/awdk/) |
| [awskills](https://github.com/Aitherium/awskills) | Portable agent skills — self-contained procedures an agent loads on demand | [docs](https://aitherium.github.io/awskills/) |
| [awpack](https://github.com/Aitherium/awpack) | First-party agent packs — the ones we build, versioned and installable on their own | [docs](https://aitherium.github.io/awpack/) |
| [awm](https://github.com/Aitherium/awm) | A portable, scoped agent memory | [docs](https://aitherium.github.io/awm/) |
| [awdesk](https://github.com/Aitherium/awdesk) | Aither World Desk -- the desktop body of AitherOS Online: tray, avatars, decision cards, the Living Desktop as an overlay | [docs](https://aitherium.github.io/awdesk/) |
| [awnode](https://github.com/Aitherium/awnode) | A lightweight local gateway — bridges your apps to the AI backends you chose | [docs](https://aitherium.github.io/awnode/) |
| [awrun](https://github.com/Aitherium/awrun) | A priority-aware queue and dispatcher for agentic runs and ad-hoc CI builds. It also judges whether the runner pool is big enough for the queue it is draining, and can ask a host to grow it -- reserving capacity is zero-sum, so a saturated pool needs more of it, not a different share of it | [docs](https://aitherium.github.io/awrun/) |
| [awgraph](https://github.com/Aitherium/awgraph) | A semantic code graph for agents — AST + tree-sitter, call graphs | [docs](https://aitherium.github.io/awgraph/) |
| [awgit](https://github.com/Aitherium/awgit) | Semantic version control on top of git — edit-ops and leases | [docs](https://aitherium.github.io/awgit/) |
| [awdelphi](https://github.com/Aitherium/awdelphi) | Anonymous multi-round expert panels — a converged answer with a trace | [docs](https://aitherium.github.io/awdelphi/) |
| [awclassify](https://github.com/Aitherium/awclassify) | Classify any document -- what it is, who may read it, who it is for, what it is about | — |
| [awtoll](https://github.com/Aitherium/awtoll) | What every tool call costs you in context, measured from your own transcripts | [docs](https://aitherium.github.io/awtoll/) |
| [awseal](https://github.com/Aitherium/awseal) | Sign an artifact so a stranger can verify it | [docs](https://aitherium.github.io/awseal/) |
| [awshare](https://github.com/Aitherium/awshare) | Publish an artifact and fetch it back verified | [docs](https://aitherium.github.io/awshare/) |
| [awdit](https://github.com/Aitherium/awdit) | An append-only audit trail whose gaps are DETECTABLE | [docs](https://aitherium.github.io/awdit/) |
| [awbac](https://github.com/Aitherium/awbac) | Role-based access control that fails closed and explains itself | [docs](https://aitherium.github.io/awbac/) |
| [awiam](https://github.com/Aitherium/awiam) | Who is this caller? A directory and session store that fails honestly | [docs](https://aitherium.github.io/awiam/) |
| [awtunnel](https://github.com/Aitherium/awtunnel) | Reach a service that has no public address | [docs](https://aitherium.github.io/awtunnel/) |
| [awnest](https://github.com/Aitherium/awnest) | Prove there is a human before you let them into the nest | [docs](https://aitherium.github.io/awnest/) |
| [awrena](https://github.com/Aitherium/awrena) | Put two agents head to head and get a verdict you can check | [docs](https://aitherium.github.io/awrena/) |
| [awnboard](https://github.com/Aitherium/awnboard) | A front gate you can put in front of anything, and hand someone the key to | [docs](https://aitherium.github.io/awnboard/) |
| [awnix](https://github.com/Aitherium/awnix) | A Linux you can hand to an agent — immutable base, capabilities included | [docs](https://aitherium.github.io/awnix/) |
| [awrecover](https://github.com/Aitherium/awrecover) | Labelled snapshots with an all-or-nothing restore | [docs](https://aitherium.github.io/awrecover/) |
| [awstorage](https://github.com/Aitherium/awstorage) | Every drive on every node, indexed, classified and diffed -- so you can see what you own before you delete it | [docs](https://aitherium.github.io/awstorage/) |
| [awrelay](https://github.com/Aitherium/awrelay) | Portable agent messaging — findings, alerts, coordination | [docs](https://aitherium.github.io/awrelay/) |
| [awask](https://github.com/Aitherium/awask) | Your agent asks you a question — and acts on your answer | [docs](https://aitherium.github.io/awask/) |
| [awmail](https://github.com/Aitherium/awmail) | Give an agent an email address — send, and actually receive | [docs](https://aitherium.github.io/awmail/) |
| [awnet](https://github.com/Aitherium/awnet) | The agentic web — agents host a mesh, and agents join one | [docs](https://aitherium.github.io/awnet/) |
| [awswarm](https://github.com/Aitherium/awswarm) | Run one model too big for any single GPU across a pool of small ones | — |
| [awfind](https://github.com/Aitherium/awfind) | A portable search client — query, results, ranking | [docs](https://aitherium.github.io/awfind/) |
| [awbrowse](https://github.com/Aitherium/awbrowse) | A portable browser client — navigate, console, network, DOM, screenshot | [docs](https://aitherium.github.io/awbrowse/) |
| [awvoice](https://github.com/Aitherium/awvoice) | Hear and speak — transcribe audio, synthesize a voice | [docs](https://aitherium.github.io/awvoice/) |
| [awvision](https://github.com/Aitherium/awvision) | See an image — describe it, ask it a question, compare two | [docs](https://aitherium.github.io/awvision/) |
| [awscreen](https://github.com/Aitherium/awscreen) | See this machine — what is on screen, and where to click it | [docs](https://aitherium.github.io/awscreen/) |
| [awkit](https://github.com/Aitherium/awkit) | Render an agent panel from a tool result — one component, any React app | — |
| [awbeads](https://github.com/Aitherium/awbeads) | A spatial canvas for a page — arrange things, connect them, and keep the arrangement | — |
| [awbonsai](https://github.com/Aitherium/awbonsai) | Run a real model in the visitor's own browser — no server round trip, no upload | — |
| [awknowledge](https://github.com/Aitherium/awknowledge) | How to run a coding agent so the result survives — the laws, with evidence | [docs](https://aitherium.github.io/awknowledge/) |
| [awbrain](https://github.com/Aitherium/awbrain) | Your history as a wiki of linked markdown — claims pinned to the evidence | — |
| [gawbbonet](https://github.com/Aitherium/gawbbonet) | GobboNet campaigns with a real agent brain — scoped memory, graph recall | [docs](https://aitherium.github.io/gawbbonet/) |
| [aitherkvcache](https://github.com/Aitherium/aitherkvcache) | Near-optimal KV cache quantization for LLM inference — sub-byte compression | [docs](https://aitherium.github.io/aitherkvcache/) |
| [awrtifact](https://github.com/Aitherium/awrtifact) | Deliberately chunk artifacts into GitHub release assets — the productized aitherkvcache mirror lane | [docs](https://aitherium.github.io/awrtifact/) |
| [AitherZero](https://github.com/Aitherium/AitherZero) | PowerShell 7+ automation framework — numbered, self-describing scripts | [docs](https://aitherium.github.io/AitherZero/) |
| [AitherConnect](https://github.com/Aitherium/AitherConnect) | Browser extension — federated AI search, page context, and the Living OS overlay | [docs](https://aitherium.github.io/AitherConnect/) |
| [awreason](https://github.com/Aitherium/awreason) | A portable reasoning client — sessions, phases, thoughts, and the chain that produced the answer | [docs](https://aitherium.github.io/awreason/) |
| [awrecurse](https://github.com/Aitherium/awrecurse) | Answer a question over a context far larger than the window — recursively, with the trace kept | [docs](https://aitherium.github.io/awrecurse/) |
| [awprism](https://github.com/Aitherium/awprism) | Turn a failure into ranked hypotheses — and say what would confirm each one | [docs](https://aitherium.github.io/awprism/) |
| [awrepl](https://github.com/Aitherium/awrepl) | A REPL an agent can actually use — state that survives between turns | [docs](https://aitherium.github.io/awrepl/) |
| [awreport](https://github.com/Aitherium/awreport) | File a bug report that has already scrubbed your secrets and collapsed the duplicate | — |
| [awresearch](https://github.com/Aitherium/awresearch) | Ask a research question, get a cited report you can check | [docs](https://aitherium.github.io/awresearch/) |
| [awfocus](https://github.com/Aitherium/awfocus) | See, search and steer every Claude session from one command | [docs](https://aitherium.github.io/awfocus/) |
| [awgym](https://github.com/Aitherium/awgym) | An ARC training gym — a game a world model can watch, and six roles that play through it | [docs](https://aitherium.github.io/awgym/) |
| [awpredict](https://github.com/Aitherium/awpredict) | Predict what your environment does next, and how surprised you were | [docs](https://aitherium.github.io/awpredict/) |
| [awevolve](https://github.com/Aitherium/awevolve) | Point an agent at a file and a command that scores it, and let it improve | — |
| [awsh](https://github.com/Aitherium/awsh) | Your terminal answers you -- type a question where a command would go | [docs](https://aitherium.github.io/awsh/) |
| [awmine](https://github.com/Aitherium/awmine) | Mine what your agents did -- outcomes, lessons and procedures out of the transcripts they left behind | — |
| **awrise** _(you are here)_ | Wake an agent on a schedule, let it do one thing, and put it back to sleep | [docs](https://aitherium.github.io/awrise/) |
| [awkno](https://github.com/Aitherium/awkno) | The man page for the Aither World — every brick, stack and law, offline | [docs](https://aitherium.github.io/awkno/) |
| [awwall](https://github.com/Aitherium/awwall) | Say what a workload may reach, and watch everything else fail closed | [docs](https://aitherium.github.io/awwall/) |
| [awrouter](https://github.com/Aitherium/awrouter) | OpenRouter for your own fleet: pick a model backend by cost/latency/ capability, fail over, fit the context window, stream. Standalone, OpenAI-compatible, no Aither-specifics required to be valuable | — |
| [awembed](https://github.com/Aitherium/awembed) | Train an embedding model that knows your corpus, and prove it beats the big one | [docs](https://aitherium.github.io/awembed/) |
| [awtax](https://github.com/Aitherium/awtax) | Turn any tax PDF -- returns, W-2, 1099, statements, even scans -- into structured data you can check | [docs](https://aitherium.github.io/awtax/) |
| [awflow](https://github.com/Aitherium/awflow) | A deterministic workflow runtime — chain agent calls with journal replay and budget control | [docs](https://aitherium.github.io/awflow/) |
| [awsettings](https://github.com/Aitherium/awsettings) | Your agent's permissions and config, following you to the next machine | [docs](https://aitherium.github.io/awsettings/) |
| [awavatar](https://github.com/Aitherium/awavatar) | One character spec in, a rigged, animated, multi-style avatar pack out | [docs](https://aitherium.github.io/awavatar/) |

<div id="aither-constellation" data-self="awrise"></div>
<script src="aither-constellation.js"></script>

<!-- aither-ecosystem:end -->
