# Engineering a Resilient Data Pipeline: Overcoming Cloud Firewalls, Race Conditions, and Schema Drift

**Project:** [NBA Analytics Pipeline](https://github.com/Cliffe16/NBA-Analytics-Pipeline) *(full architecture diagrams are in the repository's README).*
**Author:** Cliffe
**Tech Stack:** Apache Airflow, FastAPI, Tailscale (WireGuard), Google BigQuery, dbt (Astronomer Cosmos), Azure, Docker, GitHub Actions

*Disclaimer: This is a portfolio and learning project built to tackle real-world distributed-systems problems, not a hardened commercial production service. It is not affiliated with or endorsed by the NBA.*

---

### The Business Context & The "Ask"

This project started with a clear analytics goal: a daily-refreshed platform that tracks NBA player efficiency and team performance across multiple seasons. The deliverables were advanced metrics such as True Shooting % and Hollinger's Game Score, exposed through a dimensional model and a MetricFlow semantic layer, ready for dashboarding.

For scale: a regular season has 1,230 games, so `fact_game_stats` gains 2,460 rows per season (one per team per game) and `fact_player_stats` roughly 25,000 to 30,000 (about 20 to 26 player rows per game). This is small data: as of 4 November the current season stood at 107 games and 2,413 player-game rows. The hard problems in this project came from the network and the API, not from volume.

The first obstacle appeared as soon as I moved orchestration to the cloud. The NBA's stats API sits behind an Akamai Web Application Firewall that blocks traffic from cloud data-center IP ranges. In V1 I split the system in two: Apache Airflow orchestrates from an Azure VM, while extraction runs on a local Ubuntu machine on a residential connection, whose requests the firewall accepts. The two halves talk over a Tailscale (WireGuard) mesh VPN, so the worker needs no ports opened to the public internet. The full breakdown is in the [V1 case study](https://github.com/Cliffe16/NBA-Analytics-Pipeline/blob/main/docs/case_study.md).

![V1 architecture: synchronous trigger, Snowflake warehouse](https://raw.githubusercontent.com/Cliffe16/NBA-Analytics-Pipeline/main/docs/pipeline_diagram.png)

*V1 architecture: a synchronous trigger over Tailscale, loading into Snowflake.*

V1 unblocked extraction, but running it day to day exposed new problems: synchronous timeouts, overlapping runs, and storage that grew every day. V2 is the response.

### Controlling Storage Growth: The BigQuery Migration

In the original Snowflake design, each daily load appended the full season-to-date payload to the warehouse, so the same games were stored again every day.

V2 moves the warehouse to Google BigQuery and splits storage into two tiers:
1. **Static archives:** completed seasons live in `_history` tables that are written once, at season rollover.
2. **Active season:** the current season is loaded with `WRITE_TRUNCATE`, so each daily run replaces the previous one instead of adding to it.

Downstream, `fact_player_stats` is partitioned by `game_date` and clustered by `team_key` and `player_key`, so queries that filter on date or team read only the matching data.

I checked the pruning with a BigQuery dry run. `SELECT *` on `fact_player_stats` filtered to a single `game_date` reports 71.91 KB processed, against 352.98 KB for the whole table: about 80% less. At this volume the saving is academic, though. On-demand pricing bills at least 10 MB per table referenced, and a single day's partition holds only a few hundred rows, so daily partitioning here is practice for the pattern. Clustering alone would be enough at this size.

### From Synchronous Calls to Trigger-and-Poll

In V1, Airflow waited synchronously for the edge worker. The NBA API rate-limits aggressively, so long extractions dropped the connection and failed the Airflow task even though the worker was still running fine.

V2 uses an asynchronous trigger-and-poll pattern. Airflow sends a `POST` and receives `HTTP 202 Accepted` with a task ID. The extraction runs in a FastAPI background thread, task state lives in SQLite on the worker, and Airflow polls a status endpoint every 10 seconds.

Splitting a job across two machines creates new failure modes, so the worker defends against them:

- **Duplicate triggers:** if a task of the same type is already running, the API returns the existing task ID instead of starting a second extraction. An Airflow retry can never cause two concurrent runs.
- **Crash recovery:** on startup, the worker marks any task left `pending` or `running` as `failed: worker restarted`, so Airflow retries instead of polling a dead task.
- **Timeouts and retries:** each extraction task has a two-hour `execution_timeout`, and the DAG retries failed tasks three times at 10-minute intervals.

### Checkpointing the Slowest Extraction

Player biographies (`player_info`) need one API call per player, which makes them the slowest and most failure-prone extraction. The script resumes instead of restarting:

- It queries BigQuery for the players already loaded and fetches only the missing ones.
- A player that fails three attempts is skipped and picked up again on the next run. Five failures in a row are treated as an API block and abort the task.
- Whatever has been fetched is appended to BigQuery in a `finally` block, stamped with a `LOADED_AT` timestamp that staging uses to keep the latest row per player.

### The Cosmos Test Race

I use Astronomer Cosmos to run dbt because it turns each model into its own Airflow task. By default it also creates a `test` task per model, and that caused a failure I did not expect.

`dbt test --select dim_game` also selects any test that has `dim_game` as one of its parents. So the `relationships` test on `fact_game_stats.game_key` ran inside `dim_game`'s test task, before `fact_game_stats` had been rebuilt:

```
Failure in test relationships_fact_game_stats_game_key__game_key__ref_dim_game_
  Got 4930 results, configured to fail if != 0
```

The stale fact table still held game keys that the freshly built dimension no longer contained, and the DAG failed its own tests before the fact table could update. I fixed it with `RenderConfig(test_behavior=TestBehavior.AFTER_ALL)`, which builds every model first and then runs all tests together. The trade-off is that tests no longer stop downstream models from building: a failing test now fails the DAG after the marts are already built.

### Infrastructure Battles: Memory Limits & Schema Drift

- **Memory pressure on the Azure VM:** concurrent dbt compilations pushed RAM above 90%, and Airflow tasks were killed with `SIGTERM`. I limited concurrency through `docker-compose` (`PARALLELISM=2`) and set `max_active_runs=1` on the DAG.
- **Schema drift:** when the NBA API added a column to the roster payload (17 columns became 18), the static `UNION ALL` between history and daily tables broke. `dbt_utils.union_relations` now aligns the columns and pads missing ones with `NULL`s. Separately, append loads pass `ALLOW_FIELD_ADDITION`, so a new column does not fail a load into an existing table.
- **CI/CD:** on every push and pull request to `main`, GitHub Actions syncs dependencies with `uv` and runs a `pytest` suite that covers the async API. Pushes to `main` that pass are deployed to the Azure VM over SSH.

### Data Modeling Trade-offs

To track players changing teams, `dim_player` is a Slowly Changing Dimension (Type 2). My first version built team history from daily roster snapshots, and it broke because of a cost decision made elsewhere. The active-season roster table is truncated and re-stamped with today's `snapshot_date` on every run, so a traded player's `effective_start_date` on the new team moved forward to "today" on every run.

I rebuilt the history from `stg_player_game_logs` instead. That anchors each team change to the date of the player's first game for the new team, a date that never moves.

The trade-off is a known edge case. Because history is derived from games played, a player who signs with a new team in July keeps his old team in `dim_player` until his first game with the new club. For the goal here, tracking in-game performance, I accepted that.

### When the Worker Died Quietly: A 203/EXEC Crash Loop

The edge worker runs as a `systemd` service with `Restart=always`. When the absolute path to `uv` in `ExecStart` was wrong, systemd could not execute the command and failed with `status=203/EXEC`, then restarted it a few seconds later, over and over. The journal shows about 207,000 restart attempts over roughly eight days.

Airflow saw that something was wrong, but not what: `extract_stats` failed with `Connection refused` and everything downstream was skipped. Nothing in Airflow pointed at the cause, which lived on the edge node. I found it in the worker's journal.

The fix has two parts: the correct path, and `StartLimitIntervalSec=60` with `StartLimitBurst=3` in the unit's `[Unit]` section. After three quick failures systemd now stops retrying, and the unit shows up as `failed` in `systemctl --failed` instead of looping unnoticed. The lesson is that `Restart=always` can hide a startup failure for days. The worker still has no alerting when it is down, which is the first item in the next steps below.

### Results

- **Far fewer API calls:** the first `player_info` run makes about 550 calls to cover every active player on a roster. A typical daily run makes 0 to 5, for newly signed players or call-ups, which cuts the daily load on that endpoint by more than 99%.
- **Resumable extraction:** after a failure, `player_info` continues from the missing players instead of starting over, and the active-season tables are replaced on each run rather than appended to.
- **No concurrent duplicate runs:** the duplicate-trigger guard, crash recovery and timeouts mean a retry or a worker restart cannot leave Airflow polling forever or running two extractions at once.
- **Retries in practice:** on 4 November the first `game_logs` request was dropped by the API within milliseconds (`Remote end closed connection without response`). The failure reached Airflow, and the retry ten minutes later loaded 2,413 player rows and 214 team rows without any manual step.
- **Partition pruning:** a one-day query on `fact_player_stats` processes about 80% fewer bytes than a full scan (71.91 KB against 352.98 KB in a dry run). At this scale it demonstrates the pattern rather than saving real money.

### What I'd Do Next

- **Alerting:** SMTP is not configured, so a failed run only shows in the Airflow UI. A webhook `on_failure_callback` and a health check on the worker would have caught the crash loop on day one.
- **Season rollover:** archiving a season currently means editing configuration files by hand. Passing the season and an archive mode as API parameters would remove that step.
- **Securing the worker API:** it listens on all interfaces without authentication. Tailscale ACLs, or binding to the Tailscale interface and adding authentication, would close that gap.
