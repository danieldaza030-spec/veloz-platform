# Prompt: migrate compute layer from Hadoop YARN to Spark Standalone

Target agent: `coder`
Autonomy: Level 2 (controlled implementation) — ask before the two open
decisions called out below, and before any other architectural or
destructive call this prompt doesn't already resolve.

Hand this whole file to `coder` as its task.

---

<context>
Read these before changing anything — don't paste their contents back, just
use them:

- `docker-compose.yml` — the current YARN cluster block (the section
  bracketed by the "--- Hadoop YARN cluster ---" comments), plus
  `x-airflow-common`'s `HADOOP_CONF_DIR`/`YARN_CONF_DIR` env vars and the
  `hadoop-yarn-shared` bind mount.
- `docker/hadoop/Dockerfile` and `docker/hadoop/conf-templates/*.xml` — the
  bare-Hadoop image and the `envsubst`-rendered `core-site`/`yarn-site`
  config being retired.
- `plugins/spark_session.py` — `YarnSparkSessionFactory`, the class being
  replaced.
- `dags/spark_yarn_smoke_test.py` — the only current call site of
  `YarnSparkSessionFactory`.
- `.env` — `YARN_NODEMANAGER_COUNT`/`CPU_LIMIT`/`MEM_LIMIT`, the pattern to
  mirror for the new Standalone knobs.
- `docker/airflow/Dockerfile` — already installs `pyspark==3.5.3`.
  Pip-installed pyspark ships its own `sbin/start-master.sh`,
  `sbin/start-worker.sh` and `bin/spark-class`. Reusing this image for
  `spark-master`/`spark-worker` (same build, different `command:`) is very
  likely the smallest-change option — the same pattern this compose file
  already uses for the `jupyter` service. Confirm those scripts actually run
  cleanly from a pip-installed pyspark (JAVA_HOME/SPARK_HOME expectations)
  before falling back to a bespoke image like `docker/hadoop/Dockerfile`.
- `docs/ADR.md` — the Compute row (still reads "Spark in-process, no
  dedicated cluster") and the Appendix section, where this divergence needs
  to be recorded.
</context>

<instructions>

## Objective

Replace the project's Hadoop YARN compute cluster with a Spark Standalone
cluster in `docker-compose.yml`. Worker RAM/core limits must be configurable
from `.env`, the same way the YARN NodeManager pool's limits are today.
Airflow needs a `SparkSession` factory that connects to that Standalone
cluster from an explicit, injected connection config — not hardcoded values.

This is infra/compute-layer work: Docker, compose, and a plugin that only
builds and returns a `SparkSession` (no Bronze/Silver/Gold, reconciliation,
dashboard or streaming logic belongs in it). It's within your remit per
`CLAUDE.md`'s "Claude builds freely" list.

It is also a second deliberate divergence from `CLAUDE.md`'s documented
reference architecture ("Compute: Spark in-process, no dedicated cluster").
That's legitimate per `CLAUDE.md`'s own rule, provided the ADR records why.
**The fact that the compose file already diverged once, to YARN, is not
license to skip that record for this move** — see the open finding below,
which you should surface rather than quietly fold in.

## Scope

1. **Compose.** Remove `hadoop-conf-init`, `yarn-resourcemanager`,
   `yarn-nodemanager`, and `x-airflow-common`'s `HADOOP_CONF_DIR`/
   `YARN_CONF_DIR` env vars and the `hadoop-yarn-shared` bind mount. Add a
   `spark-master` service and a `spark-worker` service (`deploy.replicas`,
   the same mechanism already confirmed empirically to work for
   `yarn-nodemanager`) built from whichever image the context note above
   points you to. Worker container CPU/memory caps go through
   `deploy.resources.limits`, same as the NodeManager pool.
2. **`.env`.** Add the Standalone equivalents of `YARN_NODEMANAGER_COUNT`/
   `CPU_LIMIT`/`MEM_LIMIT` (naming is your call — match the file's existing
   comment style, which explains *why* each default is what it is, not just
   what it is). Watch the units: Spark's own `start-worker.sh` reads
   `SPARK_WORKER_MEMORY` as a Spark memory string (`"4g"`), not a bare MB
   integer the way `yarn.nodemanager.resource.memory-mb` was — carrying the
   YARN pattern's bare-number convention over here would silently
   misconfigure the worker.
3. **Port collision.** Spark's own default puts the Master web UI on 8080,
   which `airflow-apiserver` already owns on the host. Map the Master UI to a
   different host port. Worker UIs don't need a host mapping at all — YARN's
   NodeManagers didn't get one either, only the ResourceManager's 8088 did.
4. **Delete `docker/hadoop/`** (Dockerfile + conf-templates + conf-rendered)
   once nothing in compose references it. Grep for any other reference to
   `hadoop-conf`, `YARN_`, or `docker/hadoop` first, so nothing is left
   pointing at a removed path.
5. **`plugins/spark_session.py`.** Replace `YarnSparkSessionFactory` with a
   Standalone equivalent (name your call). Its constructor or `get_session()`
   must take an explicit connection/config object — not scattered
   `os.environ` reads inline — carrying at minimum the master's
   `spark://host:port` URL. Leave the existing s3a/MinIO config as-is; that
   isn't part of this change. Update `dags/spark_yarn_smoke_test.py` (the
   only current call site) to use the new class and cluster — rename the
   file/DAG id to match (e.g. `spark_standalone_smoke_test`), same shape:
   submit a real job to the cluster, print `spark.sparkContext.master` and
   the application id as proof it didn't silently fall back to local mode,
   round-trip a tiny Delta table over s3a the same way the existing smoke
   tests do.
6. **`docs/ADR.md`.** Add the dated appendix entry for this divergence.
   Before writing it: flag for the engineer, don't silently fix, that the
   *previous* divergence (YARN) never got one. `docker-compose.yml` and
   `docker/hadoop/Dockerfile` both currently cite "`docs/ADR.md`'s dated
   appendix entry" for the YARN decision, and no such entry exists —
   `docs/ADR.md`'s Compute row still reads "Spark in-process, no dedicated
   cluster," unedited. `PROGRESS.md`'s session log has no YARN entry either,
   despite compose comments citing it for empirically-verified behavior
   (`deploy.replicas` behavior, real cgroup limits). Say this plainly in your
   report; do not fold it silently into the new entry as if it always
   existed.

## Resolve before finishing the factory — don't guess

**"Credentials."** Spark Standalone has no built-in username/password auth —
unlike MinIO's IAM roles elsewhere in this stack. The closest real concept is
`spark.authenticate` + a shared secret, which nothing in this project turns
on today (including for the YARN cluster it's replacing). Ask the engineer
whether "the credentials that connect us to the Spark Standalone" means:

- (a) just the master's address/connection settings, matching the
  trust-the-docker-network posture already used everywhere else in this
  compose file, or
- (b) real authentication via `spark.authenticate.secret`, which would also
  need that secret wired into the master/worker containers' env, `.env`, and
  the factory's config object.

Don't default to (a) silently because it's less work. Ask, then build
whichever is confirmed.

**Executor sizing.** Whatever default `spark.executor.cores`/
`spark.executor.memory`/`spark.cores.max` the factory picks must not exceed
what a single worker container can actually deliver under its own cgroup
cap — the same principle the YARN setup enforced (its own compose comments:
"so YARN's scheduler never advertises more than a NodeManager container can
really deliver"). Source the default from the same `.env` values driving the
worker's limits, or require the caller to supply it explicitly with no
default. Your call — state which you picked and why.

## Constraints

- Do not touch `generators/`, any Bronze/Silver/Gold transformation,
  reconciliation, dashboard or streaming logic, or check off any row in
  `PROGRESS.md`'s platform-build table — none of that is in scope for this
  change and none of it is yours to write per `CLAUDE.md`.
- Do not edit `CLAUDE.md`'s reference-architecture table itself. The ADR
  appendix is where a divergence gets recorded; the table is the engineer's
  document to change, not yours.
- Leave the s3a/MinIO credential design (ingest vs. maintenance roles)
  exactly as it is — orthogonal to this change.

## Validate for real

Match the verification bar prior sessions in this repo already set (see
`PROGRESS.md`'s session log): bring the stack up, confirm `spark-master`'s UI
shows the expected number of registered workers with the expected
per-worker cores/memory, trigger the renamed smoke test via
`airflow dags trigger` (not a bare script call), confirm the task log shows
a `spark://` master URL (not `local[*]`) and a completed Delta round-trip,
and confirm changing the new `.env` knobs and re-running
`docker compose up -d` actually changes what the cluster reports.

## Report and record

Report file-by-file what changed and what you verified, same as always.
Update `PROGRESS.md`: check off whatever infra rows apply and write a
session-log entry — don't repeat the documentation gap found in step 6.

</instructions>
